#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# Idempotent data loader for the clickbench benchmark suite.
# Downloads ClickBench hits.parquet (14GB) if not already present.

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE_DIR="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
BENCH_SCRIPT="${WORKSPACE_DIR}/benchmarks/bench.sh"
DATA_FILE="${CLICKBENCH_DATA:-${WORKSPACE_DIR}/benchmarks/data/hits.parquet}"

if [ -f "${DATA_FILE}" ]; then
    echo "ClickBench data already exists at ${DATA_FILE}"
    exit 0
fi

echo "Downloading ClickBench hits.parquet (14GB)..."
"${BENCH_SCRIPT}" data clickbench_1
