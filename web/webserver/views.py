from django.http import HttpResponse
import logging
import re

logger = logging.getLogger(__file__)


class ValidationError(BaseException):
    pass


def index(request):
    return HttpResponse("Welcome to the ENA Monitor interface.")


def callback(request):
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

