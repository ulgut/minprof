# minprof

A streaming, multi-pass JVM heap dump analyser. `minprof` parses `.hprof` files twice, seeks past unused primitive-array payloads, keeps reusable index files on disk, computes dominators and retained sizes, then caches derived reports. A byte-heavy dump can be much larger than available RAM, while graph-heavy dumps still need memory proportional to their object and reference counts during indexing.

## Installation

```sh
cargo install --path .
```

## Usage

```
minprof <--profile <FILE> | --index-cache <DIR>> [OPTIONS]

Source (one required):
  -p, --profile <FILE>       Path to the .hprof file to parse and index
  -i, --index-cache <DIR>    Existing index directory from a previous run

Options:
  -o, --output <DIR>         Directory for index files and reports
                             [default with -p: replace .hprof with .minprof/]
      --format <FORMAT>      Output format [default: pretty]
      --report <REPORT>      Which analyses to run [default: all]
      --path <OBJECT_ID>     Print shortest reference path to this object
  -h, --help                 Print help
```

### First run

Parse and index a heap dump. Index files are written to `heap.minprof/` by default.

```sh
minprof -p heap.hprof
minprof -p heap.hprof -o /fast-disk/heap-index/
```

### Subsequent runs

Skip all parse passes and generate reports from the existing index instantly.

```sh
minprof -i heap.minprof/
minprof -i heap.minprof/ --format html
minprof -i heap.minprof/ --report leaks,packages
minprof -i /fast-disk/heap-index/ --format json
```

### `--format`

| Value    | Description |
|----------|-------------|
| `pretty` | Human-readable text tables (default) |
| `json`   | Newline-delimited JSON on stdout; progress on stderr |
| `html`   | Self-contained HTML report written to `<hprof>.html` (or `report.html` inside the index dir when using `-i`) |

### `--report`

Selects which analyses to emit. Repeatable or comma-separated. Default: `all`.

| Value       | Eclipse MAT equivalent          | Description |
|-------------|--------------------------------|-------------|
| `all`       | All reports                    | Run every analysis (default) |
| `histogram` | Class Histogram                | Object count + shallow bytes per class |
| `retained`  | Dominator Tree (by class)      | Retained heap grouped by class, top individual objects by retained size |
| `leaks`     | Leak Suspects                  | Classes retaining ≥ 1% of heap, with pattern classification |
| `packages`  | System Overview → Package view | Memory rollup by Java package |

### Examples

```sh
# First run — parse the dump, write index to heap.minprof/
minprof -p heap.hprof

# First run with custom index location
minprof -p heap.hprof -o /mnt/heap-index/

# Re-run with HTML report (no re-parsing)
minprof -i heap.minprof/ --format html

# Re-run — JSON output, retained analysis only (no re-parsing)
minprof -i heap.minprof/ --format json --report retained

# Re-run — leak suspects + packages (no re-parsing)
minprof -i heap.minprof/ --report leaks,packages

# Path to GC root for an object ID from the retained table
minprof -i heap.minprof/ --path 0x00000000d6fc57f0
```

## Output

### Text (`--format pretty`)

Four sections, each gated by `--report`:

```
Total heap: 2.45MiB shallow across 8262 objects (474 classes, 429 GC roots).
Retained heap (reachable): 237.96KiB.
Unreachable (garbage): 3533 objects, 2.22MiB — not held by any GC root.

── Top 20 classes by total allocation ──
── Top 20 classes by largest single instance ──
── Top 20 classes by retained heap ──
── Top 20 individual objects by retained heap ──  ← object IDs for --path
── Leak suspects (classes retaining ≥ 1% of heap) ──
── Top 20 packages by retained heap ──
```

The "Top 20 individual objects by retained heap" table shows object IDs you can pass directly to `--path`:

```
  0x00000000d6fc57f0  sun.misc.Launcher$AppClassLoader  retained: 48.69KiB
```

### HTML (`--format html`)

Writes a single self-contained `.html` file with no external dependencies:

- **Heap Overview** — summary cards: total shallow, retained, classes, unreachable, finalizer queue depth, soft/weak/phantom reference counts
- **GC Pressure** — reference statistics and unreachable object breakdown (Eclipse MAT "System Overview" style)
- **Leak Suspects** — numbered "Problem Suspect" cards with pattern classification
- **Retained Heap Treemap** — interactive canvas treemap; click a package to drill into its classes
- **Class Histogram** — bar charts for top classes by allocation and retained heap
- **Package Summary** — table of all packages by retained heap

### JSON (`--format json`)

Progress goes to **stderr**; only results go to **stdout** — safe to pipe or redirect.

The analysis result is a single JSON object:

```json
{
  "summary": { "total_objects": 8262, "total_classes": 474, "gc_roots": 429,
               "total_shallow_bytes": 2568192, "retained_heap_bytes": 243670,
               "unreachable_count": 3533, "unreachable_shallow_bytes": 2324480 },
  "top_allocated_classes": [
    { "class_name": "int[]", "instances": 436, "total_shallow_bytes": 2088960, "max_shallow_bytes": 649998 }
  ],
  "top_largest_instances": [ ... ],
  "retained_by_class": [
    { "class_name": "int[]", "instance_count": 436, "total_retained_bytes": 2088960,
      "total_shallow_bytes": 2088960, "avg_retained_bytes": 4793 }
  ],
  "top_retained_objects": [
    { "object_id": "0x00000000d701ec08", "class_name": "int[]",
      "shallow_bytes": 649998, "retained_bytes": 649998 }
  ],
  "leak_suspects": [
    { "class_name": "int[]", "instance_count": 436, "total_retained_bytes": 2088960,
      "avg_retained_bytes": 4793, "pct_of_heap": 81.4, "pattern": "elevated retention" }
  ],
  "package_summary": [
    { "package": "<primitive arrays>", "class_count": 5, "instance_count": 2891,
      "total_shallow_bytes": 2380800, "total_retained_bytes": 2380800 }
  ]
}
```

When `--path` is also given, a second JSON object follows on the next line ([NDJSON](https://ndjson.org/)):

```json
{"type":"path_to_root","target_id":"0x00000000d6fc57f0","found":true,"path":[
  {"object_id":"0x00000001234ab000","class_name":"java.lang.Thread","shallow_bytes":96,"is_gc_root":true,"is_target":false},
  {"object_id":"0x00000000d6fc57f0","class_name":"sun.misc.Launcher$AppClassLoader","shallow_bytes":130,"is_gc_root":false,"is_target":true}
]}
```

When the object is not found or is unreachable, `found` is `false` and an `error` key explains why.

## Index files

The first run writes the following files to the index directory (`heap.minprof/` for `heap.hprof`):

| File | Description |
|------|-------------|
| `object_index.bin` | Sorted `(object_id, class_id, shallow_size)` for every object |
| `class_names.bin` | Class name and super-class chain for every loaded class |
| `edges.bin` | Sorted `(from_id, to_id)` reference pairs |
| `shallow_sizes.bin` | Shallow bytes in object order, used by pass 4 |
| `idom.bin` | Immediate dominator array (RPO-indexed) |
| `retained.bin` | Retained size per object |
| `meta.bin` | Scalar summary (object count, heap size, unreachable stats) |
| `roots.bin` | GC root object IDs |
| `manifest.json` | Completed index generation and required-file lengths |
| `report_summary.json` | Derived report data for fast repeat queries |

`reverse_edges.bin` is generated on the first `--path` query. A completed
index is marked by `manifest.json`; `build_in_progress` rejects an interrupted
rebuild. Existing indexes without a manifest are checked and upgraded on load.

Once these files exist, `-i <dir>` can generate any report without touching the original `.hprof`.

## Memory usage

| Pass | What it does | Peak extra RAM |
|------|-------------|----------------|
| Pass 1 — index | Parse HPROF, build class index, write `object_index.bin` | strings, classes, parser buffers, and sort buffer |
| Pass 2 — edges | Extract object references and sort into `edges.bin` | parser and edge batches + sort buffer |
| Pass 3 — dominator tree | Semi-NCA algorithm on in-memory CSR graph | proportional to objects, reachable nodes, and edges |
| Pass 4 — retained sizes | Bottom-up dominator tree walk | O(N) |

Passes 3 and 4 allocate arrays proportional to object and edge counts. Array
payloads are streamed, and adjacent repeated object-array references are
elided, but unique references still increase sorter work independently of
final index size.

## Comparison with standard tooling

The usual ways to open an `.hprof` file are **Eclipse MAT**, **VisualVM**, and the
JDK's **`jhat`/`jmap`** (`jhat` is removed in modern JDKs). All three load the dump
into a heap structure sized in proportion to the dump itself, so the machine doing
the analysis needs roughly as much memory as the machine that produced the dump.
minprof streams the dump and stores the reusable index on disk, but its graph
and retained-size passes currently require memory proportional to the graph.

### Capabilities

|                         | minprof          | Eclipse MAT       | VisualVM          | jhat / jmap       |
|-------------------------|------------------|-------------------|-------------------|-------------------|
| Peak RAM                | varies with objects, references, array records, and sort buffers | ≈ dump size | ≈ dump size | ≈ dump size |
| 100 GB dump             | depends on graph shape and host resources; not validated here | needs ~100 GB RAM | needs ~100 GB RAM | impractical       |
| Retained heap           | ✅               | ✅                | ✅                | ❌                |
| Dominator tree          | ✅               | ✅                | ❌                | ❌                |
| Path to GC root         | ✅               | ✅                | ❌                | limited           |
| Leak suspects           | ✅               | ✅                | ❌                | ❌                |
| GC pressure metrics     | ✅               | ✅                | partial           | ❌                |
| Interactive treemap     | ✅ (HTML)        | ✅                | ❌                | ❌                |
| JSON / scriptable       | ✅               | ❌                | ❌                | ❌ (HTML/OQL UI)  |
| Re-query without reload | ✅ (`-i` index)  | reopen the dump   | reopen the dump   | restart the server |
| Interface              | CLI + static HTML | desktop GUI       | desktop GUI       | local web server  |

### How the approach differs

- **Memory model.** MAT and VisualVM build an in-memory object model; their working
  set scales with the dump. minprof's index lives on disk, while its temporary
  graph arrays, parser buffers, class metadata, and sort buffers consume RAM.
  A byte-heavy dump may need much less RAM than its file size; a dense graph
  can need substantially more.
- **Index once, query many.** MAT/VisualVM re-parse (or re-open the index) per
  session. minprof writes a compact on-disk index on the first run; every later
  report (`-i <dir> --report …`, `--format html|json`, `--path …`) reads only that
  index — no HPROF re-read. The index is also portable: build it on the big box,
  copy the index directory, query it without the HPROF file.
- **Scriptable by default.** Newline-delimited JSON on stdout (progress on stderr)
  drops straight into CI gates, dashboards, and diffs between two dumps. MAT is
  GUI-first (batch mode exists but is awkward); VisualVM is GUI-only.
- **No GUI, no JVM to launch.** A single static-linked binary; the HTML report is
  self-contained (no server, no external assets).

### Throughput

On a synthetic fixture of **5,000,000 objects / ~10,000,000 edges** (~0.3 GiB dump,
`gen_hprof`), the full index build — all four passes — completes in roughly
**2.4 s** end to end on a developer laptop (parse+index ~0.18 s, edges ~0.08 s,
dominator tree ~2.0 s, retained sizes ~0.08 s; `cargo bench --bench passes`). The
dominator-tree pass dominates and is the focus of ongoing optimisation. Repeat
reports off the existing index are effectively instant.

## Limitations

- CLI only — no interactive query shell
- Tested on 64-bit HotSpot HPROF format (`id_size = 8`); 32-bit dumps (`id_size = 4`) parse correctly but are less tested
- Pass 3 loads the forward graph into RAM and uses `u32` node/edge indices; large graphs can exceed memory or the index range. A 100 GiB live JVM dump has not been benchmarked on this host. See [architecture.md](architecture.md) for details.
- Hobby project — use with caution

## Design and benchmark notes

- [Current design with Mermaid diagrams](design-sketch.md)
- [Decision review, measured tradeoffs, and next experiments](design-review.md)
- [Work, memory, and disk model](benchmark-model.md)
- [Live JVM workloads and profiling](benches/README.md)
- [1–300 GB byte-heavy size sweep and chart](benches/results/byte-heavy-size-sweep.md)
- [Primitive-array seek optimization](benches/results/primitive-seek.md)
- [Contiguous-ID graph optimization](benches/results/contiguous-ids.md)
- [Semi-NCA predecessor input probe](benches/results/semi-nca-input.md)
- [100 million-object graph benchmark](benches/results/object-heavy-100m.md)

---

## References

- [hprof-slurp](https://github.com/agourlay/hprof-slurp) — single-pass streaming HPROF parser in Rust by Arnaud Gourlay. minprof's parser layer is adapted from this project (Apache 2.0).
- [HPROF format specification](https://docs.oracle.com/javase/8/docs/technotes/samples/hprof.html)
- [Georgiadis — "Linear-Time Algorithms for Dominators and Related Problems"](https://www.cs.princeton.edu/research/techreps/TR-737-05) — the Semi-NCA dominator algorithm used in pass 3
- [architecture.md](architecture.md) — full pipeline, threading model, on-disk formats, and algorithm details
