from django.views.decorators.csrf import csrf_exempt
from django.http import HttpRequest, HttpResponse, JsonResponse, Http404
from django.urls import reverse
from django.shortcuts import render, redirect
from django.utils import timezone
from django_db_logger.models import StatusLog
import rest_framework.views
from json import loads
import logging
from .models import Taxons, AccessionNumbers, RecordDetails, AssemblyStatus
from .serializers import TaxonSerializer, AccessionNumberSerializer, RecordDetailSerializer

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
        Add a new taxon_id for tracking.

        **taxon_id**: Taxonomic identifier (will include subtree)
        """
        try:
            Taxons.objects.update_or_create(taxon_id=int(taxon_id))
            logger.info(f"Added taxon id {taxon_id} via API call")
        except ValueError as e:
            logger.warning(f"Invalid taxon id '{taxon_id}' NOT ADDED.")
            return JsonResponse({'error': e}, status=400)
        return self.get(request=request, taxon_id=taxon_id, status=201, **kwargs)

    def get(self, request: HttpRequest, taxon_id: str, **kwargs):
        """
        View details of a taxon_id.

        **taxon_id**: Taxonomic identifier (will include subtree)
        """
        taxon = Taxons.objects.get(taxon_id=taxon_id)
        accessions = AccessionNumbers.objects.filter(taxon_id=taxon_id)
        taxon_serialized = TaxonSerializer(taxon)
        accessions_serialized = AccessionNumberSerializer(accessions, many=True)
        return JsonResponse({
            **taxon_serialized.data,
            'accessions': accessions_serialized.data
        }, status=kwargs['status'])


class ViewAccession(rest_framework.views.APIView):

    def _accession_details(self, accession_id: str) -> object:
        try:
            accession = AccessionNumbers.objects.get(accession_id=accession_id)
            details = RecordDetails.objects.filter(accession_id=accession_id)
        except (AccessionNumbers.DoesNotExist, RecordDetails.DoesNotExist):
            raise Http404
        return {
            **AccessionNumberSerializer(accession).data,
            'details': RecordDetailSerializer(details[0]).data
        }

    def get(self, request: HttpRequest, accession_id: str, **kwargs):
        """
        View metadata for an ENA record.

        **accession_id**: Record identifier
        """
        return JsonResponse(self._accession_details(accession_id=accession_id))

    def put(self, request: HttpRequest, accession_id: str, **kwargs):
        """
        Update metadata for an ENA record with an assembly attempt result.

        **accession_id**: Record identifier
        """
        errors = []
        data = loads(request.body)
        if not data['assembly_result']:
            errors.append('Field assembly_result must be specified.')
        elif data['assembly_result'] not in [s.value for s in AssemblyStatus]:
            errors.append(f"Unrecognised assembly_result '{data['assembly_result']}'.")

        try:
            accession = AccessionNumbers.objects.get(accession_id=accession_id)
            if accession.assembly_result != AssemblyStatus.IN_PROGRESS.value:
                errors.append(f'Record {accession_id} is not marked for assembly.')
        except AccessionNumbers.DoesNotExist:
            errors.append(f"No record found with accession_id {accession_id}")

        if len(errors) > 0:
            return JsonResponse({'error': errors}, status=400)

        accession.assembly_result = data['assembly_result']
        if data['assembled_genome_url']:
            accession.assembled_genome_url = data['assembled_genome_url']
        if data['assembly_report_url']:
            accession.assembly_report_url = data['assembly_report_url']
        accession.save()

        return HttpResponse(status=204)


class RequestAssemblyCandidate(rest_framework.views.APIView):
    def get(self, request: HttpRequest, **kwargs) -> [JsonResponse, HttpResponse]:
        """
        NON-RESTFUL - Will determine the next available record for assembly and return it.

        The record will be marked as 'under consideration'.
        If the request is not confirmed within ten minutes, the record will be unlocked and
        may be presented in response to future requests.

        Checking out a record in this way obliges you to attempt to assemble the genome and
        report the result using this API.
        """
        candidates = AccessionNumbers.objects.filter(
            waiting_since__isnull=False,
            passed_filter=True,
            assembly_result__isnull=True
        )
        if len(candidates) > 0:
            candidate = candidates.order_by('waiting_since')[0]
            candidate.assembly_result = AssemblyStatus.UNDER_CONSIDERATION.value
            candidate.waiting_since = timezone.now()
            candidate.save()
            serializer = AccessionNumberSerializer(candidate)
            return JsonResponse({
                **serializer.data,
                'accept_url': reverse('assembly_confirm', args=(candidate.accession_id,)),
                'upload_url': reverse('accession', args=(candidate.accession_id,)),
                'upload_fields': {
                    'assembly_result': {
                        'description': f"'{AssemblyStatus.FAIL.value}' or '{AssemblyStatus.SUCCESS.value}'.",
                        'required': True
                    },
                    'assembled_genome_url': {
                        'description': "URL of the assembled genomic data, if applicable.",
                        'required': False
                    },
                    'assembly_report_url': {
                        'description': "URL of the assembly report, if applicable.",
                        'required': False
                    }
                },
                'note': (
                    "This accession number is temporarily held for you. "
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
    def get(self, request: HttpRequest, accession_id: str, **kwargs) -> [JsonResponse, HttpResponse]:
        """
        Confirm assembly will proceed on an accession_id

        **accession_id**: Record identifier
        """
        accession = AccessionNumbers.objects.get(accession_id=accession_id)
        if accession.assembly_result == AssemblyStatus.UNDER_CONSIDERATION.value:
            accession.assembly_result = AssemblyStatus.IN_PROGRESS.value
            accession.waiting_since = timezone.now()
            accession.save()
            return HttpResponse(status=204)
        return JsonResponse({'error': 'Invalid confirm candidate.'}, status=400)


def healthcheck(request: HttpRequest) -> HttpResponse:
    return HttpResponse()
