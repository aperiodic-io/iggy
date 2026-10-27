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

# Message-dedup fuzz campaign: one process per seed, so no state can leak
# between runs and every failure replays with the printed command.
#
#   core/simulator/dedup-fuzz-campaign.sh <first-seed> <count> [crash-only|crash-restart|both]
set -euo pipefail
first=${1:?first seed}
count=${2:?seed count}
modes=${3:-both}
binary=$(cargo test -q --release -p simulator --lib --no-run --message-format=json 2>/dev/null |
  grep -o '"executable":"[^"]*simulator-[^"]*"' | head -1 | cut -d'"' -f4)
[[ -x "$binary" ]] || { echo "could not build the simulator test binary" >&2; exit 1; }

conclusive=0 inconclusive=0 failed=0
run() {
  local seed=$1 mode=$2 out
  local crash_only=""
  [[ $mode == crash-only ]] && crash_only=1
  if out=$(DEDUP_FUZZ_CRASH_ONLY=$crash_only DEDUP_FUZZ_SEED=$seed \
      "$binary" message_dedup_tests::dedup_fuzz_one_seed --ignored --nocapture --exact 2>&1); then
    if grep -q "conclusive: true" <<<"$out"; then conclusive=$((conclusive + 1)); else inconclusive=$((inconclusive + 1)); fi
  else
    failed=$((failed + 1))
    echo "FAILED seed=$seed mode=$mode: $(grep -E 'panicked at|twice|lost|different|diverge|gap' <<<"$out" | head -2 | cut -c1-240 | tr '\n' ' ')"
    echo "  replay: DEDUP_FUZZ_CRASH_ONLY=$crash_only DEDUP_FUZZ_SEED=$seed $binary message_dedup_tests::dedup_fuzz_one_seed --ignored --nocapture"
  fi
}
for ((seed = first; seed < first + count; seed++)); do
  [[ $modes == both || $modes == crash-only ]] && run "$seed" crash-only
  [[ $modes == both || $modes == crash-restart ]] && run "$seed" crash-restart
done
echo "campaign: $conclusive conclusive, $inconclusive inconclusive, $failed failed"
[[ $failed -eq 0 ]]
