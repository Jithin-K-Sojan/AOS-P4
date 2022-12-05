#!/bin/bash

set -euxo pipefail
function cleanup()
{
  killall mr_worker
}
trap cleanup EXIT

./mr_worker localhost:50051 &
./mr_worker localhost:50052 &
./mr_worker localhost:50053 &
./mr_worker localhost:50054 &
./mr_worker localhost:50055 &
./mr_worker localhost:50056 &

wait
