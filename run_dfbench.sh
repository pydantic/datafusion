#!/bin/bash
# Wrapper: called as "./run_dfbench.sh --bin dfbench -- <args>"
# Strips "--bin dfbench --" and calls $DFBENCH_BIN with remaining args
shift 3  # remove --bin dfbench --
exec "$DFBENCH_BIN" "$@"
