"""An example HTTP server with GET and POST endpoints."""

# https://gist.github.com/nitaku/10d0662536f37a087e1b?permalink_comment_id=3375622#gistcomment-3375622

from http.server import HTTPServer, BaseHTTPRequestHandler
from http import HTTPStatus
import json
import re
import logging
import os
import click
from pathlib import Path

from . import log_format as log_format
from . import Route as Route


logger = logging.getLogger(__file__)
queue_dir = ""


class _RequestHandler(BaseHTTPRequestHandler):

    class ValidationError(BaseException):
        pass


    # Borrowing from https://gist.github.com/nitaku/10d0662536f37a087e1b
    def _set_headers(self, status=HTTPStatus.OK):
        self.send_response(status.value)
        self.send_header('Content-type', 'text/plain; charset=UTF-8')
        # Allow requests from any origin, so CORS policies don't
        # prevent local development.
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()

    def do_GET(self):
        logger.info(f"GET request received.")
        self._set_headers()
        self.send_response(b"Go away.")

    def do_POST(self):
        logger.info(f"Request {self.request}")
        try:
            length = int(self.headers.get('content-length'))
            content = self.rfile.read(length)
            logger.debug(content)
            message = json.loads(content)
            message['taxon_id'] = str(message['taxon_id'])
            # Process message body
            if message['taxon_id'] is None:
                raise self.ValidationError(f"taxon_id must be present")
            if re.match(
                r"^((([0-1]?[0-9]?[0-9])|(2[0-4][0-9])|(25[0-4]))\.){3}(([0-1]?[0-9]?[0-9])|(2[0-4][0-9])|(25[0-4]))$",
                message['droplet_ip']
            ) is None:
                raise self.ValidationError(f"droplet_ip must be an IPv4 address, received {message['droplet_ip']}")
            if message['status'] != "ready" and message['status'] != "error":
                raise self.ValidationError(f"unrecognised status, received {message['status']}")
            if message['status'] == "error":
                if message['error'] is not str:
                    raise self.ValidationError("error cannot be empty when status = 'error'")
            else:
                if message['error'] != "":
                    raise self.ValidationError(
                        f"error must be blank when status != 'error', received {message['error']}"
                    )
        except self.ValidationError as e:
            message = None
            logger.error(e)
            self._set_headers(HTTPStatus.BAD_REQUEST)
            self.wfile.write(b"Error")
        except Exception as e:
            message = None
            self._set_headers(HTTPStatus.BAD_REQUEST)
            self.wfile.write(b"Error")
            logger.error(e)

        if message:
            try:
                request_command = (
                    f"update-droplet-status {message['taxon_id']} {message['droplet_ip']} {message['status']}"
                )
                logger.info(f"Request: {request_command}")
                Path(os.path.join(queue_dir, request_command)).touch(exist_ok=True)

                self._set_headers()
                self.wfile.write(b"OK")
                logger.info(f"Request processed successfully")
            except Exception as e:
                self._set_headers(status=HTTPStatus.INTERNAL_SERVER_ERROR)
                self.wfile.write(b"Error")
                logger.error(f"Error processing valid request -- {e}")

    def do_OPTIONS(self):
        # Send allow-origin header for preflight POST XHRs.
        self.send_response(HTTPStatus.NO_CONTENT.value)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'POST')
        self.send_header('Access-Control-Allow-Headers', 'content-type')
        self.end_headers()


def run_server(ctx: click.Context):
    global queue_dir
    queue_dir = os.path.join(ctx.obj['DATA_DIR'], Route.QUEUE_DIR.value)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(log_format)
    logger.addHandler(stream_handler)

    log_location = Path(os.path.join(ctx.obj['LOG_DIR'], Route.SERVER_LOG.value))
    # Create the directory skeleton if necessary
    os.makedirs(os.path.dirname(log_location), exist_ok=True)
    log_location.touch(exist_ok=True)
    file_handler = logging.FileHandler(filename=log_location, encoding='utf-8')
    file_handler.setFormatter(log_format)
    logger.addHandler(file_handler)

    logger.setLevel(logging.DEBUG if ctx.obj['VERBOSE'] else logging.INFO)
    server_address = ('', 80)
    httpd = HTTPServer(server_address, _RequestHandler)
    logger.info('Server started. Listening at %s:%d' % server_address)
    httpd.serve_forever()


if __name__ == '__main__':
    run_server({'obj': {'DATA_DIR': '../../data', 'LOG_DIR': '../../data/log', 'VERBOSE': False}})
