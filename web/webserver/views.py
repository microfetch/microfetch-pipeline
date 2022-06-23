from django.utils.datastructures import MultiValueDictKeyError
from django.http import HttpRequest, HttpResponse, JsonResponse, Http404
from django.urls import reverse
from django.shortcuts import render, redirect
from django.utils import timezone
from django.db.models import Q, QuerySet
from django_db_logger.models import StatusLog
from rest_framework.response import Response
from functools import wraps
import rest_framework.decorators
import rest_framework.views
import rest_framework.viewsets
import rest_framework.exceptions
import rest_framework.mixins
import json
import logging
from .models import Taxons, Records, AssemblyStatus, QualifyrReport, name_map, qualifyr_name_map
from .serializers import TaxonSerializer, RecordSerializer

logger = logging.getLogger(__file__)


class ValidationError(BaseException):
    pass


def paginate(func):
    """
    Allow ModelViewSet custom list actions to be paginated.
    https://stackoverflow.com/a/56609821
    """
    @wraps(func)
    def inner(self, *args, **kwargs):
        queryset = func(self, *args, **kwargs)
        assert isinstance(queryset, (list, QuerySet)), "apply_pagination expects a List or a QuerySet"

        page = self.paginate_queryset(queryset)
        if page is not None:
            serializer = self.get_serializer(page, many=True)
            return self.get_paginated_response(serializer.data)

        serializer = self.get_serializer(queryset, many=True)
        return Response(serializer.data)
    return inner


LOG_LEVEL_NAMES = {
    logging.NOTSET: 'NotSet',
    logging.DEBUG: 'Debug',
    logging.INFO: 'Info',
    logging.WARNING: 'Warning',
    logging.ERROR: 'Error',
    logging.FATAL: 'Fatal'
}


def parse_log_entry(log_entry: StatusLog) -> str:
    level = LOG_LEVEL_NAMES[log_entry.level] if log_entry.level in LOG_LEVEL_NAMES.keys() else 'UNKNOWN'
    return f"{log_entry.create_datetime} {log_entry.logger_name} ({level}):\t{log_entry.msg}"


def index(request: HttpRequest) -> HttpResponse:
    log_entries = StatusLog.objects.all().order_by('-create_datetime')  # TODO: limit to one page of logs
    log = [parse_log_entry(x) for x in log_entries if x.level >= logging.INFO]

    return render(
        request,
        "webserver/home.html",
        {
            'messages': log
        }
    )


def add_taxon_id(taxon_id: int) -> None:
    try:
        Taxons.objects.update_or_create(taxon_id=int(taxon_id))
        logger.info(f"Added taxon id {taxon_id}")
    except ValueError:
        logger.warning(f"Invalid taxon id '{taxon_id}' NOT ADDED.")


def post(request: HttpRequest) -> HttpResponse:
    if 'taxon_ids' in request.POST.keys():
        taxon_ids = request.POST['taxon_ids'].split(',')
        for taxon_id in taxon_ids:
            add_taxon_id(int(taxon_id))
    return redirect(reverse("index"))


class HyperlinkedViewSet(rest_framework.viewsets.GenericViewSet):
    """
    Wrapper for GenericViewSet that adds the request into the serializer context.
    """
    def get_serializer_context(self):
        context = super(HyperlinkedViewSet, self).get_serializer_context()
        context.update({"request": self.request})
        return context


class TaxonViewSet(
    rest_framework.viewsets.ModelViewSet,
    HyperlinkedViewSet
):
    serializer_class = TaxonSerializer
    queryset = Taxons.objects.all()


class RecordViewSet(rest_framework.viewsets.ReadOnlyModelViewSet, HyperlinkedViewSet):
    serializer_class = RecordSerializer

    def _filter_queryset(
            self,
            taxon_id: list = None,
            assembly_status: list = None,
            passed_screening: list = None,
            passed_filter: list = None
    ):
        def _compile_values(cs_list: list) -> tuple:
            options = []
            if 'true' in cs_list:
                options.append(True)
            if 'false' in cs_list:
                options.append(False)
            return 'null' in cs_list, options

        queryset = Records.objects.all()
        if taxon_id is not None:
            queryset = queryset.filter(taxon_id__in=taxon_id)
        if assembly_status is not None:
            queryset = queryset.filter(assembly_result__in=assembly_status)
        if passed_screening is not None:
            null, values = _compile_values(passed_screening)
            if null:
                queryset = queryset.filter(Q(passed_screening__isnull=True) | Q(passed_screening__in=values))
            else:
                queryset = queryset.filter(passed_screening__in=values)
        if passed_filter is not None:
            null, values = _compile_values(passed_filter)
            if null:
                queryset = queryset.filter(Q(passed_filter__isnull=True) | Q(passed_filter__in=values))
            else:
                queryset = queryset.filter(passed_filter__in=values)
        return queryset

    # List methods
    def get_queryset(self):
        """
        Returns Records matching the querystring parameters.

        Available querystring arguments (all optional):

        **taxon_id**: Taxon identifier; comma-separate for multiple

        **assembly_status**: Values: 'waiting', 'skipped', 'in_progress', 'success', 'fail'; comma-separate for multiple

        **passed_screening**: Values: 'true', 'false', 'null'; comma-separated

        **passed_filter**: Values: 'true', 'false', 'null'; comma-separated
        """
        try:
            taxon_id = self.request.query_params.get('taxon_id').split(',')
        except AttributeError:
            taxon_id = None
        try:
            assembly_status = self.request.query_params.get('assembly_status').replace('_', ' ').lower().split(',')
        except AttributeError:
            assembly_status = None
        try:
            passed_screening = self.request.query_params.get('passed_screening').lower().split(',')
        except AttributeError:
            passed_screening = None
        try:
            passed_filter = self.request.query_params.get('passed_filter').lower().split(',')
        except AttributeError:
            passed_filter = None
        return self._filter_queryset(
            taxon_id=taxon_id,
            assembly_status=assembly_status,
            passed_screening=passed_screening,
            passed_filter=passed_filter
        )

    @rest_framework.decorators.action(methods=['get'], detail=False, serializer_class=RecordSerializer)
    @paginate
    def awaiting_assembly(self, request: HttpRequest, **kwargs):
        """
        View records which are awaiting assembly.

        Related action endpoints are included in each record's **action_links**.
        These action links allow assembly pipeline runners to interface effectively
        with the API:

        If a record awaiting assembly is accepted for an assembly attempt,
        this API should be notified to avoid redundant assembly attempts.
        The API can be notified by sending an **HTTP GET request** to the
        `/records/{id}/register_assembly_attempt/` endpoint.

        Records marked as undergoing assembly will not be included in future
        requests to this endpoint for some time.
        The hold duration depends on runtime configuration, but is 24 hours by default.

        Once assembly is complete, the API should be notified of the result
        using the **POST** `/records/{id}/report_assembly_result/` endpoint.
        That endpoint accepts JSON input with the following fields:

        **assembly_result** [required]: 'success' or 'fail'

        **assembled_genome_url**: URL of the assembled genomic data, if applicable.

        **assembly_error_report_url**: URL of the nextflow pipeline error log for failed runs.

        **assembly_error_process**: Name (and tag) of failing pipeline process.

        **assembly_error_exit_code**: Error code of the failing process.

        **assembly_error_stdout**: Output of the failing process.

        **assembly_error_stderr**: Error report from the failing process.

        **qualifyr_report**: JSON representation of the assembly qualifyr_report.tsv file.
        For a full list of compatible fields, **GET** `/qualifyr_report_fields/`.

        """
        return self._filter_queryset(assembly_status=['waiting'])

    @rest_framework.decorators.action(methods=['get'], detail=False)
    @paginate
    def awaiting_screening(self, request: HttpRequest, **kwargs):
        """
        View records which are awaiting screening.
        """
        return self._filter_queryset(assembly_status=['success'], passed_screening=['false'])

    @rest_framework.decorators.action(methods=['get'], detail=False)
    @paginate
    def screened(self, request: HttpRequest, **kwargs):
        """
        View records which have passed screening.
        """
        return self._filter_queryset(passed_screening=['true'])

    # Update methods
    @rest_framework.decorators.action(methods=['get'], detail=True)
    def register_assembly_attempt(self, request: HttpRequest, pk: str, **kwargs):
        """
        Mark a record as undergoing assembly.

        Records marked as undergoing assembly will not be included in future
        requests to this endpoint for some time.
        The hold duration depends on runtime configuration, but is 24 hours by default.
        """
        record = Records.objects.get(id=pk)
        if record.assembly_result == AssemblyStatus.WAITING.value:
            record.assembly_result = AssemblyStatus.IN_PROGRESS.value
            record.waiting_since = timezone.now()
            record.save()
            return Response(self.get_serializer(record, context=self.get_serializer_context()).data)
        raise rest_framework.exceptions.APIException('Invalid assembly candidate. This record may already be reserved.')

    @rest_framework.decorators.action(methods=['post'], detail=True)
    def report_assembly_result(self, request: HttpRequest, pk: str, **kwargs):
        """
        Update metadata for an ENA record with an assembly result.

        POST content should be JSON with the following fields:

        **assembly_result** [required]: 'success' or 'fail'

        **assembled_genome_url**: URL of the assembled genomic data, if applicable.

        **assembly_error_report_url**: URL of the nextflow pipeline error log for failed runs.

        **assembly_error_process**: Name (and tag) of failing pipeline process.

        **assembly_error_exit_code**: Error code of the failing process.

        **assembly_error_stdout**: Output of the failing process.

        **assembly_error_stderr**: Error report from the failing process.

        **qualifyr_report**: JSON representation of the assembly qualifyr_report.tsv file.
        For a full list of compatible fields, **GET** `/qualifyr_report_fields/`.
        """
        errors = []
        data = request.data
        if 'assembly_result' not in data.keys():
            errors.append('Field assembly_result must be specified.')
        elif data['assembly_result'] not in [s.value for s in AssemblyStatus]:
            errors.append(f"Unrecognised assembly_result '{data['assembly_result']}'.")

        try:
            record = Records.objects.get(id=pk)
            if record.assembly_result != AssemblyStatus.IN_PROGRESS.value:
                errors.append(f'Record {pk} is not marked for assembly.')
        except Records.DoesNotExist:
            errors.append(f"No record found with id {pk}")

        if len(errors) > 0:
            raise rest_framework.exceptions.APIException(errors)

        record.assembly_result = data['assembly_result']
        if 'assembled_genome_url' in data.keys():
            record.assembled_genome_url = data['assembled_genome_url']
        if 'assembly_error_report_url' in data.keys():
            record.assembly_error_report_url = data['assembly_error_report_url']
        if 'assembly_error_process' in data.keys():
            record.assembly_error_process = data['assembly_error_process']
        if 'assembly_error_exit_code' in data.keys():
            record.assembly_error_exit_code = data['assembly_error_exit_code']
        if 'assembly_error_stdout' in data.keys():
            record.assembly_error_stdout = data['assembly_error_stdout']
        if 'assembly_error_stderr' in data.keys():
            record.assembly_error_stderr = data['assembly_error_stderr']
        record.save()

        # Map the qualifyr_report to a database entry if it exists
        if 'qualifyr_report' in data.keys() and data['qualifyr_report']:
            report = {}
            qualifyr_report = json.loads(data['qualifyr_report'])
            for k, v in qualifyr_report.items():
                report[name_map(k)] = v
            logger.debug(report)
            QualifyrReport.objects.create(record_id=record.id, **report)

        return Response(self.get_serializer(record, context=self.get_serializer_context()).data)

    @rest_framework.decorators.action(methods=['post'], detail=True)
    def report_screening_result(self, request: HttpRequest, pk: str, **kwargs):
        """
        Update metadata for an ENA record with a screening result.

        POST content should be JSON and include the fields:

        **passed_screening** [required]: boolean (or string representation) of whether
        the screening process was passed for the record.
        **screening_message**: String with an (optional) message to provide more details
        about the screening. If the screening fails, it can be useful to specify a reason
        in this field.
        """
        errors = []
        data = request.data
        if 'passed_screening' not in data.keys():
            errors.append('Field passed_screening must be specified.')
        else:
            passed_screening = data['passed_screening']
            if isinstance(passed_screening, str):
                passed_screening = True if passed_screening.lower()[0] == "t" else False
            elif not isinstance(passed_screening, bool):
                errors.append(f"Unrecognised passed_screening value: '{data['passed_screening']}'.")

        try:
            record = Records.objects.get(id=pk)
        except Records.DoesNotExist:
            errors.append(f"No record found with id {pk}")

        if len(errors) > 0:
            raise rest_framework.exceptions.APIException(errors)

        record.passed_screening = passed_screening
        if 'screening_message' in data.keys():
            record.screening_message = data['screening_message']
        record.save()

        return Response(self.get_serializer(record, context=self.get_serializer_context()).data)


class QualifyrReportFields(rest_framework.views.APIView):
    def get(self, request: HttpRequest, **kwargs) -> JsonResponse:
        """
        A list of all acceptable fields for inclusion in a Qualifyr Report upload.
        """
        return JsonResponse([v for _, v in qualifyr_name_map.items()], safe=False)


def healthcheck(request: HttpRequest) -> HttpResponse:
    return HttpResponse()
