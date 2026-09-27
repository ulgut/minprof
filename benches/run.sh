#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -lt 4 ] || [ "$#" -gt 7 ]; then
  echo "Usage: benches/run.sh <name> <heap-MiB> <direct-MiB> <graph-nodes> [block-MiB] [array-refs] [shared|alternating|unique]" >&2
  echo "Example: benches/run.sh heap-1g 1024 0 100000" >&2
  exit 2
fi

name=$1
heap_mib=$2
direct_mib=$3
nodes=$4
block_mib=${5:-8}
array_refs=${6:-0}
array_mode=${7:-shared}
case "$array_mode" in
  shared|alternating|unique) ;;
  *) echo "array mode must be shared, alternating, or unique" >&2; exit 2 ;;
esac
case "$name" in
  *[!a-zA-Z0-9_-]*|'') echo "name must use letters, digits, _ or -" >&2; exit 2 ;;
esac
for number in "$heap_mib" "$direct_mib" "$nodes" "$block_mib" "$array_refs"; do
  case "$number" in
    *[!0-9]*|'') echo "sizes and node count must be nonnegative integers" >&2; exit 2 ;;
  esac
done
if [ "$block_mib" -lt 1 ] || [ "$block_mib" -gt 1024 ]; then
  echo "block-MiB must be between 1 and 1024" >&2
  exit 2
fi

# Avoid launching a live allocation that clearly exceeds RAM plus swap. This
# is a coarse capacity check, not a guarantee that other processes leave room.
capacity_mib=0
available_mib=0
if [ "$(uname)" = Darwin ]; then
  pressure=$(memory_pressure -Q 2>/dev/null || true)
  ram_bytes=$(printf '%s\n' "$pressure" | awk '/^The system has / {print $4}')
  free_pct=$(printf '%s\n' "$pressure" | awk '/System-wide memory free percentage:/ {print $5}' | tr -d '%')
  swap_status=$(sysctl vm.swapusage 2>/dev/null || true)
  swap_total=$(printf '%s\n' "$swap_status" | sed -n 's/.*total = \([0-9.]*\)M.*/\1/p')
  swap_free=$(printf '%s\n' "$swap_status" | sed -n 's/.*free = \([0-9.]*\)M.*/\1/p')
  if [[ "$ram_bytes" =~ ^[0-9]+$ ]]; then
    capacity_mib=$((ram_bytes / 1048576))
    if [[ "$free_pct" =~ ^[0-9]+$ ]]; then
      available_mib=$((capacity_mib * free_pct / 100))
    fi
  fi
  if [[ "$swap_total" =~ ^[0-9]+([.][0-9]+)?$ ]]; then
    capacity_mib=$((capacity_mib + ${swap_total%.*}))
  fi
  if [[ "$swap_free" =~ ^[0-9]+([.][0-9]+)?$ ]]; then
    available_mib=$((available_mib + ${swap_free%.*}))
  fi
elif [ -r /proc/meminfo ]; then
  capacity_mib=$(awk '/MemTotal:|SwapTotal:/ {sum += $2} END {print int(sum / 1024)}' /proc/meminfo)
  available_mib=$(awk '/MemAvailable:|SwapFree:/ {sum += $2} END {print int(sum / 1024)}' /proc/meminfo)
fi
array_refs_mib=$(((array_refs * 8 + 1048575) / 1048576))
array_targets_mib=0
if [ "$array_mode" = unique ]; then
  array_targets_mib=$(((array_refs * 24 + 1048575) / 1048576))
fi
requested_mib=$((heap_mib + direct_mib + array_refs_mib + array_targets_mib))
if [ "$capacity_mib" -gt 0 ] && [ "$requested_mib" -gt "$((capacity_mib * 80 / 100))" ]; then
  echo "Requested live bytes exceed 80% of RAM plus swap (${capacity_mib} MiB). Use a larger host." >&2
  exit 1
fi
if [ "$available_mib" -gt 0 ] && [ "$((requested_mib + 512))" -gt "$available_mib" ]; then
  echo "Requested live bytes plus 512 MiB JVM headroom exceed available RAM and swap (${available_mib} MiB)." >&2
  exit 1
fi

if [ "$(uname)" = Darwin ]; then
  time_cmd=(/usr/bin/time -l)
else
  time_cmd=(/usr/bin/time -v)
fi

project_root=$(cd "$(dirname "$0")/.." && pwd)
build_dir="$project_root/target/bench-java"
run_dir="$project_root/target/real-bench/$name"
mkdir -p "$build_dir" "$run_dir"
dump="$run_dir/workload.hprof"
index="$run_dir/index"
if [ -e "$dump" ] || [ -e "$index" ]; then
  echo "Refusing to overwrite existing benchmark output in $run_dir" >&2
  exit 1
fi

heap_limit_mib=$((heap_mib + array_refs_mib + array_targets_mib + nodes / 10000 + 768))
direct_limit_mib=$((direct_mib + 256))
javac -d "$build_dir" "$project_root/benches/HeapWorkload.java"
cargo build --release --manifest-path "$project_root/Cargo.toml" --bin minprof

{
  echo "name=$name"
  echo "heap_mib=$heap_mib"
  echo "direct_mib=$direct_mib"
  echo "nodes=$nodes"
  echo "block_mib=$block_mib"
  echo "array_refs=$array_refs"
  echo "array_mode=$array_mode"
  java -version 2>&1
  git -C "$project_root" rev-parse HEAD
  date -u +%Y-%m-%dT%H:%M:%SZ
  echo "capacity_mib=$capacity_mib"
  echo "available_mib=$available_mib"
  df -h "$run_dir"
} > "$run_dir/metadata.txt"

"${time_cmd[@]}" java -Xms256m -Xmx"${heap_limit_mib}m" \
  -XX:MaxDirectMemorySize="${direct_limit_mib}m" \
  -cp "$build_dir" HeapWorkload \
  --heap-mib "$heap_mib" --direct-mib "$direct_mib" --nodes "$nodes" \
  --block-mib "$block_mib" \
  --array-refs "$array_refs" \
  --array-mode "$array_mode" \
  --dump "$dump" > "$run_dir/java.stdout" 2> "$run_dir/java.time"

"${time_cmd[@]}" "$project_root/target/release/minprof" \
  -p "$dump" -o "$index" --format json \
  > "$run_dir/minprof.json" 2> "$run_dir/minprof.time"

"${time_cmd[@]}" "$project_root/target/release/minprof" \
  -i "$index" --format json \
  > "$run_dir/cache.json" 2> "$run_dir/cache.time"

du -sh "$dump" "$index" > "$run_dir/sizes.txt"
echo "Benchmark outputs: $run_dir"
