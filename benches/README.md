# Live JVM benchmark workloads

`HeapWorkload.java` creates a live HotSpot heap dump. Three independent inputs distinguish the questions we want to measure:

| Input | What it stresses | What HPROF contains |
|---|---|---|
| `--heap-mib` | Large on-heap byte arrays; dump I/O and parsing | The byte arrays and their Java owners |
| `--direct-mib` | Native memory held by direct buffers | Small `ByteBuffer` wrappers; native bytes are absent |
| `--nodes` | Object count and reference graph | `GraphNode` objects and edges |
| `--array-refs`, `--array-mode` | One large object array with shared, alternating, or unique targets | Separates scanned references, emitted sort records, and distinct edges |

Every allocated 4 KiB page is written so resident memory is committed. `benches/run.sh` builds the Java workload and release `minprof`, times heap-dump creation, fresh indexing, and an index-only query, and records output sizes. It keeps results under ignored `target/real-bench/<name>/`; it refuses to overwrite a run. Use a new name to repeat a measurement.

```sh
benches/run.sh heap-1g 1024 0 100000
benches/run.sh direct-1g 0 1024 100000
benches/run.sh heap-10g 10240 0 1000000
benches/run.sh direct-10g 0 10240 1000000
benches/run.sh heap-100g 102400 0 1000000
benches/run.sh heap-1g-large-arrays 1024 0 1000 256
benches/run.sh one-large-object-array 0 0 0 8 16000000
benches/run.sh alternating-object-array 0 0 0 8 16000000 alternating
benches/run.sh unique-object-array 0 0 0 8 1000000 unique
```

The optional fifth argument sets the heap/direct allocation block size in MiB
(default 8). Hold total heap bytes and graph nodes fixed while varying this
argument to isolate the effect of individual array-record size. The optional
sixth argument creates a single object array of that many references. The
optional seventh chooses `shared` (default), `alternating` (two targets with
no adjacent repeats), or `unique` (one target object per slot). These cases
separate array scanning, early elision, sorter volume, and final edge count.

For a 100 GiB **synthetic HPROF** with a small graph and rooted byte arrays:

```sh
cargo build --release --bin gen_hprof --bin minprof
target/release/gen_hprof --output target/byte-heavy-100g-rooted.hprof \
  --objects 1000 --classes 1 --roots 1 --byte-array-mib 102400 \
  --byte-array-block-mib 256
python3 -B benches/profile_run.py target/byte-heavy-100g-rooted.hprof \
  target/profile-byte-heavy-100g-rooted
```

The writer uses zero-filled, 1 MiB chunks and roots each array. This fixture
occupies about 100 GiB on disk, stays within HPROF's 32-bit segment and array
length fields, and produces a tiny index. It measures large-file I/O and
parser behavior, not codec ratios on realistic bytes or graph memory growth.
Before primitive-array seeking, the measured build took 46.96 s at 271 MB
peak RSS; passes 1 and 2 each spent about 23.4 s reading the 100 GiB dump.
The current reader skips those payloads. The integrated 100 GiB comparison
fell from 46.96 s to 0.80 s with byte-identical output. See
[the seek result](results/primitive-seek.md) for details.

An earlier header-only seek experiment counted all 400 arrays and 100 GiB of
payload in 0.070 s on this host. It skipped other record bodies and did no
indexing, so the integrated result above is the relevant production measure.

The same fixture shape was also measured at 1, 10, 30, and 280 GiB. The last
size is 300.65 GB decimal and requires roughly 280 GiB of free disk while
its raw HPROF exists:

```sh
target/release/gen_hprof --output target/byte-heavy-300gb-rooted.hprof \
  --objects 1000 --classes 1 --roots 1 --byte-array-mib 286720 \
  --byte-array-block-mib 256
python3 -B benches/profile_run.py target/byte-heavy-300gb-rooted.hprof \
  target/profile-byte-heavy-300gb-rooted
python3 -B benches/plot_size_sweep.py
```

The [size-sweep report and SVG](results/byte-heavy-size-sweep.md) include the
measured table and limitations. The raw inputs were archived after profiling
as `target/byte-heavy-*.hprof.zst` with `zstd -t` verification, then removed
to restore disk headroom. Profile summaries and indexes remain under
`target/profile-byte-heavy-*`. Use new output names for a repeat run because
`profile_run.py` refuses to overwrite a prior profile.

The last command is a workload definition, not a promise that the host can run it. The Java process must hold the live allocation while writing the dump, and the dump plus index and temporary files need disk space. This machine currently reports 18 GiB physical memory and 6 GiB swap, so a 100 GiB live allocation is outside its configured capacity. Use `src/bin/gen_hprof.rs` for 100 GiB *file-scale* indexing measurements here, and a larger host for a 100 GiB *live JVM* measurement. A direct-memory run tests JVM/native RSS, while the HPROF and minprof should remain small because native bytes are not serialized in the heap dump.

The runner checks total and currently available RAM plus swap before starting Java. It reserves at least 512 MiB for JVM overhead, but this is still a coarse safety check: other processes can change memory pressure during a run. On this host the 10 GiB example currently fails that check; it can run when enough memory becomes available.

For result comparisons, record the host, Java version, Rust commit, free disk, memory pressure, cold or warm cache state, dump size, object/edge counts, pass times, and peak RSS. The runner captures `/usr/bin/time -l` on macOS or `/usr/bin/time -v` on Linux for each process. Do not compare byte-heavy and graph-heavy workloads as if file size alone explains indexing cost.

## Full-build profiling

`profile_run.py` profiles a fresh build from an existing HPROF. It timestamps
pass and pass-3 phase boundaries, samples process RSS and index-directory
bytes every 250 ms, and records the OS-reported whole-process peak RSS. The
sampled phase peaks and disk high-water mark are lower bounds. The wrapper
also records process CPU time, page faults, and swap counters plus system-wide
swap snapshots (which other processes can affect). It needs `ps` access; some
sandboxes require an unsandboxed benchmark run.

```sh
python3 -B benches/profile_run.py target/real-bench/graph-20m/workload.hprof \
  target/profile-graph-20m-repeat
```

Pass 3 automatically uses direct range lookup when object IDs are contiguous;
other indexes use sort and join. A synthetic dense spill fixture is deliberately
different from the live-Java graph and should be labeled synthetic in results:

`profile_run.py --sort-mib N` sets the experimental sorter chunk limit to
`N` MiB. Compare it with the default on the same HPROF; the override affects
every external sort in the build.

The default reader seeks over primitive payloads. The prior pipelined reader
was removed after the full-build comparisons in
[the seek result](results/primitive-seek.md).

```sh
target/release/gen_hprof --output target/real-bench/synthetic-65m-8e/workload.hprof \
  --objects 65000000 --classes 1100 --obj-fields 8 \
  --prim-fields 2 --null-pct 0 --roots 1000
```

The [Semi-NCA input result](results/semi-nca-input.md) records the focused
read-method comparison; production uses the bounded block scan. Historical
compression and summary-cache experiments are documented in
[`design-review.md`](../design-review.md).
