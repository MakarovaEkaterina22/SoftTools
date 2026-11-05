#!/bin/sh
set -eu
docker compose down
# Named volumes are retained to preserve your results.
