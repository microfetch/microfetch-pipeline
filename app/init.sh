#!/bin/sh
# init.sh

# adapted from https://docs.docker.com/compose/startup-order/

set -e

host="$1"
shift
port="$1"
shift
shift

until wget -qO- http://"$host":"$port"/healthcheck/; do
  >&2 echo "Webserver is unavailable - sleeping"
  sleep 1
done

>&2 echo "Webserver ready - running command $*"
exec "$@"
