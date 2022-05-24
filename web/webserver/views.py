from django.views.decorators.csrf import csrf_exempt
from django.http import HttpRequest, HttpResponse, JsonResponse
from django.urls import reverse
from django.shortcuts import render, redirect
from django.utils import timezone
from django_db_logger.models import StatusLog
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


def api_taxons(request: HttpRequest) -> JsonResponse:
    """
    GET View all tracked taxons.
    """
    if request.method == 'GET':
        taxons = Taxons.objects.all()
        serializer = TaxonSerializer(taxons, many=True)
        return JsonResponse(serializer.data, safe=False)


@csrf_exempt
def api_taxon(request: HttpRequest, taxon_id: str) -> JsonResponse:
    """
    PUT track a new taxon_id
        201 - Created (returns GET data)
        400 - Error during creation
    GET view taxon_id record and a list of its related accession_ids
        200 - OK
    """
    status = 200
    if request.method == 'PUT':
        try:
            Taxons.objects.update_or_create(taxon_id=int(taxon_id))
            status = 201
            logger.info(f"Added taxon id {taxon_id} via API call")
        except ValueError as e:
            logger.warning(f"Invalid taxon id '{taxon_id}' NOT ADDED.")
            return JsonResponse({'error': e}, status=400)

    if request.method == 'GET' or status == 201:
        taxon = Taxons.objects.get(taxon_id=taxon_id)
        accessions = AccessionNumbers.objects.filter(taxon_id=taxon_id)
        taxon_serialized = TaxonSerializer(taxon)
        accessions_serialized = AccessionNumberSerializer(accessions, many=True)
        return JsonResponse({
            **taxon_serialized.data,
            'accessions': accessions_serialized.data
        }, status=status)


def _accession_details(accession_id: str) -> object:
    accession = AccessionNumbers.objects.get(accession_id=accession_id)
    details = RecordDetails.objects.filter(accession_id=accession_id)
    return {
        **AccessionNumberSerializer(accession).data,
        'details': RecordDetailSerializer(details[0]).data
    }


@csrf_exempt
def api_accession(request: HttpRequest, accession_id: str) -> [JsonResponse, HttpResponse]:
    """
    GET view a full accession record
        200 - OK
    PUT update an accession record with an assembly result
        204 - Updated
    """
    if request.method == 'GET':
        return JsonResponse(_accession_details(accession_id=accession_id))

    if request.method == 'PUT':
        errors = []
        data = loads(request.body)
        if not data['assembly_result']:
            errors.append('Field assembly_result must be specified.')
        elif data['assembly_result'] not in [s.value for s in AssemblyStatus]:
            errors.append(f"Unrecognised assembly_result '{data['assembly_result']}'.")
        if len(errors) > 0:
            return JsonResponse(errors, status=400)

        accession = AccessionNumbers.objects.get(accession_id=accession_id)
        accession.assembly_result = data['assembly_result']
        if data['assembled_genome_url']:
            accession.assembly_genome_url = data['assembled_genome_url']
        if data['assembly_report_url']:
            accession.assembly_report_url = data['assembly_report_url']
        accession.save()

        return HttpResponse(status=204)


def api_request_assembly_candidate(request: HttpRequest) -> [JsonResponse, HttpResponse]:
    """
    GET receive details of an accession awaiting assembly
        200 - OK
        204 - No accessions awaiting assembly
    """
    if request.method == 'GET':
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


def api_confirm_assembly_candidate(request: HttpRequest, accession_id: str) -> [JsonResponse, HttpResponse]:
    """
    GET confirm assembly will proceed on an accession_id
        204 - OK
        400 - Error
    """
    if request.method == 'GET':
        accession = AccessionNumbers.objects.get(accession_id=accession_id)
        if accession.assembly_result == AssemblyStatus.UNDER_CONSIDERATION.value:
            accession.assembly_result = AssemblyStatus.IN_PROGRESS.value
            accession.waiting_since = timezone.now()
            accession.save()
            return HttpResponse(status=204)
        return JsonResponse({'error': 'Invalid confirm candidate.'}, status=400)


def healthcheck(request: HttpRequest) -> HttpResponse:
    return HttpResponse()
