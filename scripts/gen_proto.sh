#!/usr/bin/env bash
# Generates Python gRPC stubs from proto/incident_store.proto into services/common/s19common/pb/
# Usage: scripts/gen_proto.sh [repo_root]   (the Dockerfile calls it with /src)
set -euo pipefail
ROOT="${1:-$(cd "$(dirname "$0")/.." && pwd)}"
OUT="$ROOT/services/common/s19common/pb"
mkdir -p "$OUT"
python -m grpc_tools.protoc -I "$ROOT/proto" \
  --python_out="$OUT" --grpc_python_out="$OUT" \
  "$ROOT/proto/incident_store.proto"
# protoc emits "import incident_store_pb2" - make it a package-relative import
sed -i 's/^import incident_store_pb2 as/from s19common.pb import incident_store_pb2 as/' \
  "$OUT/incident_store_pb2_grpc.py"
touch "$OUT/__init__.py"
echo "generated stubs in $OUT"
