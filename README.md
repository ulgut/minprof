# minprof

A streaming, multi-pass JVM heap dump analyzer. For huge (XX-XXXGB) heaps, Eclipse MAT and VisualVM need to load the entire heap into RAM and are a pain to work with.
`minprof` allows the processing of `.hprof` files many times larger than available physical RAM on resource-constrained devices (e.g. 16GB laptops), without sacrificing any insight that tools 
like Eclipse MAT and VisualVM provide. `minprof` achieves this by trading loading time for memory by streaming the file in multiple passes and keeping intermediate data on disk.

## Installation

```
cargo install --path .
```

## Usage

```
minprof <HPROF> [--output <DIR>] [--path <OBJECT_ID>] [--json]
```

`<HPROF>` — path to the `.hprof` file.

`--output` — directory for intermediate index files. Defaults to
`<hprof>.minprof/` next to the dump. The directory is reusable: if you re-run
minprof on the same dump, it overwrites previous results.

`--path <OBJECT_ID>` — print the shortest reference chain from a GC root to
the given object. The ID is the hex address shown in the output tables
(e.g. `--path 0x7f3a1c80` or `--path 7f3a1c80`).

`--json` — emit results as JSON instead of formatted text (see [JSON output](#json-output)).

### Example

```
$ minprof heap.hprof
output dir: heap.minprof
=== pass 1: build index ===
  482301 objects, 3847 classes, 413 roots
=== pass 2: extract edges ===
  1923847 references
=== pass 3: dominator tree ===
=== pass 4: retained sizes ===
pass 4: done — 48.3 MB retained heap across 482301 objects (12847 unreachable, 1.9 MB garbage)

Found a total of 50.20MiB of instances allocated on the heap (482301 objects, 3847 classes).
Retained heap of reachable objects: 48.30MiB (413 GC roots).
Unreachable (garbage) objects: 12847 objects, 1.99MiB shallow — not reachable from any GC root.

Top 20 allocated classes:
+----------------+-----------+---------------+------------------------------------------------+
| Total size     | Instances |       Largest | Class name                                     |
+----------------+-----------+---------------+------------------------------------------------+
| 12.34MiB       |    204819 |    192.00bytes | byte[]                                         |
...
```

### Path to GC root

Copy an object ID from the retained table, then:

```
$ minprof heap.hprof --path 0x7f3a1c80

Path from GC root to 0x00000007f3a1c80:

  0x00000001234ab000  java.lang.Thread                                  (shallow: 96.00bytes) ← GC root
  → 0x00000003fab12400  java.util.concurrent.ThreadPoolExecutor         (shallow: 64.00bytes)
  → 0x00000005c8891200  java.util.concurrent.LinkedBlockingQueue        (shallow: 48.00bytes)
  → 0x00000007f3a1c80   com.example.RequestContext                      (shallow: 128.00bytes) ← target
```

## JSON output

Progress messages always go to **stderr**; only result data goes to **stdout**.
This makes stdout safe to redirect or pipe without filtering in any mode.

`--json` switches stdout to JSON. The analysis result is one JSON object:

```json
{
  "summary": {
    "total_objects": 482301,
    "total_classes": 3847,
    "gc_roots": 413,
    "total_shallow_bytes": 52674560,
    "retained_heap_bytes": 50661376,
    "unreachable_count": 12847,
    "unreachable_shallow_bytes": 2086912
  },
  "top_allocated_classes": [
    {"class_name": "byte[]", "instances": 204819, "total_shallow_bytes": 12939264, "max_shallow_bytes": 192}
  ],
  "top_largest_instances": [ ... ],
  "top_retained": [
    {"class_name": "com.example.Foo", "shallow_bytes": 128, "retained_bytes": 8388608}
  ]
}
```

When `--path` is also given, a second JSON object follows on the next line ([NDJSON](https://ndjson.org/)):

```json
{"type":"path_to_root","target_id":"0x00000007f3a1c80","found":true,"path":[
  {"object_id":"0x00000001234ab000","class_name":"java.lang.Thread","shallow_bytes":96,"is_gc_root":true,"is_target":false},
  {"object_id":"0x00000007f3a1c80","class_name":"com.example.RequestContext","shallow_bytes":128,"is_gc_root":false,"is_target":true}
]}
```

When the object is not found or is unreachable, `found` is `false` and an `error` key describes why.

## How to capture a heap dump

**Running process:**
```
jcmd <pid> GC.heap_dump /tmp/heap.hprof
# or
jmap -dump:format=b,file=/tmp/heap.hprof <pid>
```

**On OutOfMemoryError** (add to JVM flags):
```
-XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/tmp/heap.hprof
```

## Memory usage

| Pass | Peak RAM |
|------|----------|
| Pass 1 — parse & index | ~100 MB (class index) + I/O buffer |
| Pass 2 — edges | same |
| Pass 3 — dominator tree | I/O buffer + OS page cache |
| Pass 4 — retained sizes | I/O buffer |

The class index is the only structure kept in RAM across passes. For real-world
JVM applications this is typically under 100 MB regardless of heap size.
I/O buffer defaults to 64 MB (configurable at compile time).

## Comparison

|                      | minprof       | Eclipse MAT   | VisualVM      |
|----------------------|---------------|---------------|---------------|
| Peak RAM             | ~1 GB         | ≈ dump size   | ≈ dump size   |
| 100 GB dump          | works         | needs 100 GB RAM | needs 100 GB RAM |
| Retained heap        | ✅            | ✅            | ✅            |
| Dominator tree       | ✅            | ✅            | ❌            |
| Path to GC root      | ✅            | ✅            | ❌            |
| JSON / scriptable    | ✅            | ❌            | ❌            |
| GUI                  | ❌ (CLI only) | ✅            | ✅            |

## Limitations

- CLI only — no GUI or interactive query shell
- Tested on 64-bit HotSpot HPROF format (id_size = 8); 32-bit dumps (id_size = 4) are parsed correctly, but less tested
- Currently a hobby project, use with caution
---

## References

- [hprof-slurp](https://github.com/agourlay/hprof-slurp) — single-pass streaming
  HPROF parser in Rust by Arnaud Gourlay. minprof's parser layer is adapted from
  this project (Apache 2.0). hprof-slurp benchmarks at ~1–2 GB/s parse throughput
  and is the basis for our streaming architecture.
- [HPROF format specification](https://docs.oracle.com/javase/8/docs/technotes/samples/hprof.html)
- [Cooper, Harvey, Kennedy — "A Simple, Fast Dominance Algorithm"](https://www.cs.tufts.edu/~nr/cs257/archive/keith-cooper/dom14.pdf) — the iterative dominator algorithm used in pass 3
