from django.utils.datastructures import MultiValueDictKeyError
from django.http import HttpRequest, HttpResponse, JsonResponse, Http404
from django.urls import reverse
from django.shortcuts import render, redirect
from django.utils import timezone
from django_db_logger.models import StatusLog
import rest_framework.views
import json
import logging
from .models import Taxons, Records, RecordDetails, AssemblyStatus, QualifyrReport, name_map, qualifyr_name_map
from .serializers import TaxonSerializer, RecordSerializer, RecordDetailSerializer

logger = logging.getLogger(__file__)


class ValidationError(BaseException):
    pass


LOG_LEVEL_NAMES={
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


class ListTaxons(rest_framework.views.APIView):

    def get(self, request: HttpRequest) -> JsonResponse:
        """
        View all tracked taxons.
        """
        taxons = Taxons.objects.all()
        serializer = TaxonSerializer(taxons, many=True)
        return JsonResponse(serializer.data, safe=False)


class ViewTaxon(rest_framework.views.APIView):
    def put(self, request: HttpRequest, taxon_id: str, **kwargs) -> JsonResponse:
        """
        Add a new id for tracking.

        **id**: Taxonomic identifier (will include subtree)
        """
        try:
            if "filters" in request.data.keys() and 'filters' in request.data['filters'].keys():
                filters = {"filters": request.data['filters']}
            else:
                filters = None
            Taxons.objects.update_or_create(
                id=int(taxon_id),
                post_assembly_filters=filters
            )
            logger.info(f"Added taxon id {taxon_id} via API call")
        except (ValueError, MultiValueDictKeyError) as e:
            logger.warning(f"Invalid taxon id '{taxon_id}' NOT ADDED.")
            return JsonResponse({'error': e}, status=400)
        return self.get(request=request, taxon_id=taxon_id, status=201, **kwargs)

    def get(self, request: HttpRequest, taxon_id: str, status: int = 200, **kwargs):
        """
        View details of a id.

        **id**: Taxonomic identifier (will include subtree)
        """
        taxon = Taxons.objects.get(id=taxon_id)
        records = Records.objects.filter(taxon_id=taxon_id)
        taxon_serialized = TaxonSerializer(taxon)
        records_serialized = RecordSerializer(records, many=True)
        return JsonResponse({
            **taxon_serialized.data,
            'records': records_serialized.data
        }, status=status)


class ViewRecord(rest_framework.views.APIView):

    def _record_details(self, record_id: str) -> object:
        try:
            accession = Records.objects.get(id=record_id)
            details = RecordDetails.objects.filter(record_id=record_id)
        except (Records.DoesNotExist, RecordDetails.DoesNotExist):
            raise Http404
        return {
            **RecordSerializer(accession).data,
            'details': RecordDetailSerializer(details[0]).data
        }

    def get(self, request: HttpRequest, record_id: str, **kwargs):
        """
        View metadata for an ENA record.

        **id**: Record identifier
        """
        return JsonResponse(self._record_details(record_id=record_id))

    def put(self, request: HttpRequest, record_id: str, **kwargs):
        """
        Update metadata for an ENA record with an assembly attempt result.

        **id**: Record identifier
        """
        errors = []
        data = request.data
        if not 'assembly_result' in data.keys():
            errors.append('Field assembly_result must be specified.')
        elif data['assembly_result'] not in [s.value for s in AssemblyStatus]:
            errors.append(f"Unrecognised assembly_result '{data['assembly_result']}'.")

        try:
            record = Records.objects.get(id=record_id)
            if record.assembly_result != AssemblyStatus.IN_PROGRESS.value:
                errors.append(f'Record {record_id} is not marked for assembly.')
        except Records.DoesNotExist:
            errors.append(f"No record found with id {record_id}")

        if len(errors) > 0:
            return JsonResponse({'error': errors}, status=400)

        record.assembly_result = data['assembly_result']
        if 'assembled_genome_url' in data.keys():
            record.assembled_genome_url = data['assembled_genome_url']
        if 'assembly_error_report_url' in data.keys():
            record.assembly_error_report_url = data['assembly_error_report_url']
        record.save()

        # Map the qualifyr_report to a database entry if it exists
        if 'qualifyr_report' in data.keys() and data['qualifyr_report']:
            report = {}
            qualifyr_report = json.loads(data['qualifyr_report'])
            for k, v in qualifyr_report.items():
                report[name_map(k)] = v
            logger.debug(report)
            QualifyrReport.objects.create(record_id=record.id, **report)

        return HttpResponse(status=204)


class RequestAssemblyCandidate(rest_framework.views.APIView):
    def get(self, request: HttpRequest, **kwargs) -> [JsonResponse, HttpResponse]:
        """
        NON-RESTFUL - Will determine the next available record for assembly and return it.
        TODO: Would be more restful to return all, let client choose, and have client reserve via
            a GET /api/confirm_assembly_candidate/ (can return non-200 status code if already reserved).

        The record will be marked as 'under consideration'.
        If the request is not confirmed within ten minutes, the record will be unlocked and
        may be presented in response to future requests.

        Checking out a record in this way obliges you to attempt to assemble the genome and
        report the result using this API.
        """
        candidates = Records.objects.filter(
            waiting_since__isnull=False,
            passed_filter=True,
            assembly_result__isnull=True
        )
        if len(candidates) > 0:
            candidate = candidates.order_by('waiting_since')[0]
            candidate.assembly_result = AssemblyStatus.UNDER_CONSIDERATION.value
            candidate.waiting_since = timezone.now()
            candidate.save()
            serializer = RecordSerializer(candidate)
            try:
                filters = Taxons.objects.get(taxon_id=candidate.taxon_id_id).post_assembly_filters
            except BaseException:
                filters = None

            return JsonResponse({
                **serializer.data,
                'post_assembly_filters': filters,
                'accept_url': reverse('assembly_confirm', args=(candidate.id,)),
                'upload_url': reverse('record', args=(candidate.id,)),
                'upload_fields': {
                    'assembly_result': {
                        'description': f"'{AssemblyStatus.FAIL.value}' or '{AssemblyStatus.SUCCESS.value}'.",
                        'required': True
                    },
                    'assembled_genome_url': {
                        'description': "URL of the assembled genomic data, if applicable.",
                        'required': False
                    },
                    'assembly_error_report_url': {
                        'description': "URL of the nextflow pipeline error log for failed runs.",
                        'required': False
                    },
                    'qualifyr_report': {
                        'description': (
                            "JSON representation of the assembly qualifyr_report.tsv file. "
                            f"For a full list of compatible fields, GET {reverse('qualifyr_report_fields')}."
                        ),
                        'required': False
                    }
                },
                'note': (
                    "This record number is temporarily held for you. "
                    "If you do not send a GET request to the accept_url "
                    "within 10 minutes, the API will assume that you do not wish "
                    "to continue assembling this record. "
                    "Please send a GET request to the accept_url if you decide "
                    "to attempt assembly.\n"
                    "Sending this request means you also promise to upload your "
                    "results to the API by sending the data via PUT request to "
                    f"the upload_url. "
                    f"The PUT request payload should be JSON; see upload_fields for the fields."
                )
            })
        else:
            return HttpResponse(status=204)


class AcceptAssemblyCandidate(rest_framework.views.APIView):
    def get(self, request: HttpRequest, record_id: str, **kwargs) -> [JsonResponse, HttpResponse]:
        """
        Confirm assembly will proceed on an id

        **id**: Record identifier
        """
        accession = Records.objects.get(id=record_id)
        if accession.assembly_result == AssemblyStatus.UNDER_CONSIDERATION.value:
            accession.assembly_result = AssemblyStatus.IN_PROGRESS.value
            accession.waiting_since = timezone.now()
            accession.save()
            return HttpResponse(status=204)
        return JsonResponse({'error': 'Invalid confirm candidate.'}, status=400)


class QualifyrReportFields(rest_framework.views.APIView):
    def get(self, request: HttpRequest, **kwargs) -> JsonResponse:
        """
        A list of all acceptable fields for inclusion in a Qualifyr Report upload.
        """
        return JsonResponse([v for _, v in qualifyr_name_map.items()], safe=False)


def healthcheck(request: HttpRequest) -> HttpResponse:
    return HttpResponse()
