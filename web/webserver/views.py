from django.http import HttpRequest, HttpResponse
from django.urls import reverse
from django.shortcuts import render, redirect
import logging
import re
from django_db_logger.models import StatusLog
from .models import taxons, accession_numbers, record_details

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
        taxons.objects.update_or_create(taxon_id=int(taxon_id))
        logger.info(f"Added taxon id {taxon_id}")
    except ValueError:
        logger.warning(f"Invalid taxon id '{taxon_id}' NOT ADDED.")


def post(request: HttpRequest) -> HttpResponse:
    if 'taxon_ids' in request.POST.keys():
        taxon_ids = request.POST['taxon_ids'].split(',')
        for taxon_id in taxon_ids:
            add_taxon_id(int(taxon_id))
    return redirect(reverse("index"))


def callback(request: HttpRequest) -> HttpResponse:
    logger.info(f"Request {request}")
    try:
        message = request.POST
        logger.debug(message)
        message['taxon_id'] = str(message['taxon_id'])
        # Process message body
        if message['taxon_id'] is None:
            raise ValidationError(f"taxon_id must be present")
        if re.match(
                r"^((([0-1]?[0-9]?[0-9])|(2[0-4][0-9])|(25[0-4]))\.){3}(([0-1]?[0-9]?[0-9])|(2[0-4][0-9])|(25[0-4]))$",
                message['droplet_ip']
        ) is None:
            raise ValidationError(f"droplet_ip must be an IPv4 address, received {message['droplet_ip']}")
        if message['status'] != "ready" and message['status'] != "error":
            raise ValidationError(f"unrecognised status, received {message['status']}")
        if message['status'] == "error":
            if message['error'] is not str:
                raise ValidationError("error cannot be empty when status = 'error'")
        else:
            if message['error'] != "":
                raise ValidationError(
                    f"error must be blank when status != 'error', received {message['error']}"
                )
    except ValidationError as e:
        return HttpResponse(e, status=400)
    except Exception as e:
        return HttpResponse(e, status=500)

    if message:
        try:
            request_command = (
                f"update-droplet-status {message['taxon_id']} {message['droplet_ip']} {message['status']}"
            )
            logger.info(f"Request: {request_command}")
            # TODO: some Postgres operation

            logger.info(f"Request processed successfully")
            return HttpResponse("OK")
        except Exception as e:
            logger.error(f"Error processing valid request -- {e}")
            return HttpResponse(e, status=500)
