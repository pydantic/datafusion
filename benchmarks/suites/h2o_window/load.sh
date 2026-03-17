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

# Idempotent data loader for the h2o_window benchmark suite.
# Generates H2O join CSV data (small/1e7 rows) if not already present.
# Window benchmarks use the "large" table from the join dataset (J1_1e7_1e7_NA.csv).

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE_DIR="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
BENCH_SCRIPT="${WORKSPACE_DIR}/benchmarks/bench.sh"
DATA_DIR="${H2O_DATA:-${WORKSPACE_DIR}/benchmarks/data/h2o}"

if [ -f "${DATA_DIR}/J1_1e7_1e7_NA.csv" ]; then
    echo "H2O window data already exists at ${DATA_DIR}"
    exit 0
fi

echo "Generating H2O join data (includes window dataset)..."
"${BENCH_SCRIPT}" data h2o_small_join
