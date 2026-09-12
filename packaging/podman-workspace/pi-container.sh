#!/bin/sh
set -eu
payload=${PI_FRAMEWORK_PAYLOAD:?}
exec node "$payload/pi-runtime/dist/bundle/cli.js" "$@"
