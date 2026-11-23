#!/usr/bin/env bash
# Runs every benchmark into results/, from the jar built by: ./mvnw -T1C clean package -DskipTests
# BENCHMARK_CPUS and BENCHMARK_PAIR_CPUS pin the runs to quiet cores (taskset -c lists); unset, they are not pinned.
set -euo pipefail
cd "$(dirname "$0")"

DATE=$(date +%Y-%m-%d)
JAVA="${JAVA_HOME:+$JAVA_HOME/bin/}java"
JAVA_MAJOR=$("$JAVA" -XshowSettings:properties -version 2>&1 | awk -F'= ' '/java.specification.version/ {print $2}')
CPUS="${BENCHMARK_CPUS:-}"
PAIR_CPUS="${BENCHMARK_PAIR_CPUS:-$CPUS}"

# run <cpus> <result name> <jmh arguments...>: the log is kept too, where ReplicationLoadBenchmark writes its figures.
run() {
  local cpus=$1 name="$2-jdk${JAVA_MAJOR}-${DATE}"
  shift 2
  local pin=()
  if [ -n "$cpus" ]; then
    pin=(taskset -c "$cpus")
  fi
  "${pin[@]}" "$JAVA" -jar target/benchmarks.jar -rf json -rff "results/jmh-result-$name.json" -prof gc "$@" 2>&1 \
    | tee "results/$name.log"
}

for PROVIDER in \
    "PROVIDER_JDK CLEAR,AES_GCM_128,AES_GCM_256,CHACHA20_POLY1305,AES_CTR_HMAC_SHA256,CHACHA20_HMAC_SHA256" \
    "PROVIDER_CORRETTO AES_GCM_128,AES_GCM_256" \
    "PROVIDER_BC AES_GCM_128,AES_GCM_256,CHACHA20_POLY1305,CHACHA20_HMAC_SHA256" \
    "PROVIDER_BC_LTS AES_GCM_128,AES_GCM_256,CHACHA20_POLY1305"; do
  set -- $PROVIDER
  run "$CPUS" "FrameCipherBenchmark-$1" FrameCipherBenchmark -p provider="$1" -p encryption="$2"
done
run "$CPUS" ControlFrameCipherBenchmark ControlFrameCipherBenchmark

run "$PAIR_CPUS" ReplicationRttBenchmark ReplicationRttBenchmark
run "$PAIR_CPUS" ReplicationRttBenchmark-spinning ReplicationRttBenchmark -p followerSink=SINK_MEMORY -p waiting=SPIN
run "$PAIR_CPUS" ReplicationRttBenchmark-encryption ReplicationRttBenchmark -p mode=SYNC_APPLIED \
    -p followerSink=SINK_MEMORY -p encryption=CLEAR,AES_GCM_256,AES_CTR_HMAC_SHA256,CHACHA20_HMAC_SHA256

run "$PAIR_CPUS" ReplicationLoadBenchmark ReplicationLoadBenchmark
