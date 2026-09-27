//! Synthetic HPROF heap dump generator for benchmarking minprof.
//!
//! Produces a structurally valid HPROF binary with configurable scale —
//! identical parsing paths to a real JVM dump, but generated in seconds
//! rather than requiring a live Java process.
//!
//! # ID layout
//!   [1, C]          class object IDs
//!   [C+1, C+N]      instance object IDs
//!   [C+N+1, ...]    optional primitive byte-array IDs
//!   [after arrays, ...] string IDs (field names, class names)
//!
//! # Typical usage
//!
//!   # ~32 GiB local file (500 M objects, ~1 B edges after 50% null)
//!   cargo run --release --bin gen_hprof -- --output test.hprof
//!
//!   # HPROF format scale (~445 GiB; graph exceeds minprof's current u32 limit)
//!   cargo run --release --bin gen_hprof -- --output large.hprof --objects 7760000000
//!
//!   # 100 GiB of array payload with a small graph (file-scale parser test)
//!   cargo run --release --bin gen_hprof -- --output byte-heavy.hprof \
//!     --objects 1000 --classes 1 --roots 1 --byte-array-mib 102400

use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::PathBuf;

use clap::Parser;

#[derive(Parser)]
#[command(about = "Generate a synthetic HPROF file for minprof benchmarking")]
struct Args {
    /// Output file path
    #[arg(long, default_value = "test.hprof")]
    output: PathBuf,

    /// Total number of instance objects
    #[arg(long, default_value_t = 500_000_000)]
    objects: u64,

    /// Number of Java classes
    #[arg(long, default_value_t = 24_000)]
    classes: u64,

    /// Object-typed instance fields per class (each non-null one becomes an edge)
    #[arg(long, default_value_t = 4)]
    obj_fields: u32,

    /// Int-typed instance fields per class (increases shallow size, no edges)
    #[arg(long, default_value_t = 2)]
    prim_fields: u32,

    /// Percentage of Object field slots that are null (0–100)
    #[arg(long, default_value_t = 50)]
    null_pct: u32,

    /// Number of GC roots (ROOT_JNI_GLOBAL records)
    #[arg(long, default_value_t = 18_253)]
    roots: u32,

    /// Total MiB of zero-filled primitive byte-array payload to append
    #[arg(long, default_value_t = 0)]
    byte_array_mib: u64,

    /// Size of each synthetic byte array, in MiB (1–1024)
    #[arg(long, default_value_t = 256)]
    byte_array_block_mib: u32,
}

const WRITE_BUF: usize = 64 * 1024 * 1024;
const MIB: u64 = 1024 * 1024;

fn main() {
    let args = Args::parse();
    assert!(args.classes > 0, "--classes must be positive");
    assert!(
        (1..=1024).contains(&args.byte_array_block_mib),
        "--byte-array-block-mib must be between 1 and 1024"
    );
    let byte_array_bytes = args
        .byte_array_mib
        .checked_mul(MIB)
        .expect("--byte-array-mib is too large");
    let array_block_bytes = u64::from(args.byte_array_block_mib) * MIB;
    let array_count = byte_array_bytes.div_ceil(array_block_bytes);
    assert!(array_count <= u32::MAX as u64, "too many byte arrays");

    let per_obj = instance_sub_record_size(args.obj_fields, args.prim_fields) as u64;
    let avg_edges = args.obj_fields as f64 * (100 - args.null_pct) as f64 / 100.0;

    eprintln!("Generating synthetic HPROF");
    eprintln!("  output:       {:?}", args.output);
    eprintln!("  objects:      {}", args.objects);
    eprintln!("  classes:      {}", args.classes);
    eprintln!(
        "  obj fields:   {} ({}% null → {:.1} avg edges/obj, ~{:.1} B total edges)",
        args.obj_fields,
        args.null_pct,
        avg_edges,
        args.objects as f64 * avg_edges / 1e9,
    );
    eprintln!("  prim fields:  {}", args.prim_fields);
    eprintln!("  roots:        {}", args.roots);
    eprintln!(
        "  byte arrays:  {} MiB across {} arrays ({} MiB each)",
        args.byte_array_mib, array_count, args.byte_array_block_mib
    );
    eprintln!(
        "  est. size:    {:.1} GiB",
        (args.objects as f64 * per_obj as f64 + byte_array_bytes as f64) / (1u64 << 30) as f64
    );

    let t0 = std::time::Instant::now();
    let f = File::create(&args.output).expect("create output file");
    let mut w = BufWriter::with_capacity(WRITE_BUF, f);

    generate(&mut w, &args).expect("generation failed");
    w.flush().expect("flush failed");

    let size = std::fs::metadata(&args.output)
        .map(|m| m.len())
        .unwrap_or(0);
    let secs = t0.elapsed().as_secs_f64();
    eprintln!(
        "Done: {:.1} GiB in {:.1}s ({:.0} MiB/s)",
        size as f64 / (1u64 << 30) as f64,
        secs,
        size as f64 / secs / (1u64 << 20) as f64,
    );
}

// ── Top-level generation ──────────────────────────────────────────────────────

fn generate(w: &mut impl Write, a: &Args) -> std::io::Result<()> {
    let c = a.classes;
    let n = a.objects;
    let class_base: u64 = 1;
    let inst_base: u64 = c + 1;
    let byte_array_bytes = a.byte_array_mib * MIB;
    let array_block_bytes = u64::from(a.byte_array_block_mib) * MIB;
    let array_count = byte_array_bytes.div_ceil(array_block_bytes);
    let array_base = c + n + 1;
    let str_base: u64 = array_base + array_count;
    let total_fields = a.obj_fields + a.prim_fields;

    // ── HPROF file header ─────────────────────────────────────────────────────
    // "JAVA PROFILE 1.0.2\0" (null-terminated) + id_size(u32 BE) + timestamp(u64 BE)
    w.write_all(b"JAVA PROFILE 1.0.2\0")?;
    w.write_all(&8u32.to_be_bytes())?;
    w.write_all(&0u64.to_be_bytes())?;

    // ── UTF-8 strings ─────────────────────────────────────────────────────────
    // Field names ("f0", "f1", …) — shared across all classes.
    let mut field_name_ids = Vec::with_capacity(total_fields as usize);
    for fi in 0..total_fields {
        let sid = str_base + fi as u64;
        write_utf8(w, sid, format!("f{fi}").as_bytes())?;
        field_name_ids.push(sid);
    }
    // Class names in JVM internal format ("com/example/Gen{i}").
    let mut class_name_ids = Vec::with_capacity(c as usize);
    for ci in 0..c {
        let sid = str_base + total_fields as u64 + ci;
        write_utf8(w, sid, format!("com/example/Gen{ci}").as_bytes())?;
        class_name_ids.push(sid);
    }

    // ── LoadClass records ─────────────────────────────────────────────────────
    for ci in 0..c {
        write_load_class(
            w,
            ci as u32 + 1,
            class_base + ci,
            class_name_ids[ci as usize],
        )?;
    }

    // ── ClassDump segment ─────────────────────────────────────────────────────
    let class_sub_sz = class_dump_sub_record_size(total_fields) as u64;
    let class_seg_body = c * class_sub_sz;
    assert!(
        class_seg_body <= u32::MAX as u64,
        "ClassDump segment exceeds u32::MAX — reduce --classes"
    );
    write_seg_hdr(w, class_seg_body as u32)?;
    let instance_data_size = a.obj_fields * 8 + a.prim_fields * 4;
    for ci in 0..c {
        write_class_dump(
            w,
            class_base + ci,
            /*super_id=*/ 0,
            /*instance_size=*/ 16 + instance_data_size, // JVM header + field data
            a.obj_fields,
            a.prim_fields,
            &field_name_ids,
        )?;
    }

    // ── GC roots segment ──────────────────────────────────────────────────────
    // Root the byte arrays as well: a live-heap dump would retain them.
    // ROOT_JNI_GLOBAL: tag(1) + oid(8) + jni_ref(8) = 17 bytes each.
    let root_seg_body = (a.roots as u64 + array_count) * 17;
    assert!(root_seg_body <= u32::MAX as u64);
    write_seg_hdr(w, root_seg_body as u32)?;
    let mut rng = Rng::new(0xdeadbeef_cafebabe);
    // JNI ref IDs live just above the object ID space.
    let jni_base = str_base + total_fields as u64 + c;
    for ri in 0..a.roots as u64 {
        let oid = inst_base + rng.range(n);
        write_gc_root_jni_global(w, oid, jni_base + ri)?;
    }
    for ai in 0..array_count {
        write_gc_root_jni_global(w, array_base + ai, jni_base + a.roots as u64 + ai)?;
    }

    // ── InstanceDump segments ─────────────────────────────────────────────────
    // Batch so each segment's body length fits in u32.
    let per_obj = instance_sub_record_size(a.obj_fields, a.prim_fields) as u64;
    let batch_max: u64 = u32::MAX as u64 / per_obj;
    let mut written = 0u64;
    let mut rng = Rng::new(0x1234567890abcdef);

    while written < n {
        let batch = (n - written).min(batch_max);
        let seg_body = batch * per_obj;
        write_seg_hdr(w, seg_body as u32)?;

        for i in 0..batch {
            let oid = inst_base + written + i;
            let class_id = class_base + (written + i) % c;
            write_instance_dump(
                w,
                oid,
                class_id,
                a.obj_fields,
                a.prim_fields,
                a.null_pct,
                inst_base,
                n,
                &mut rng,
            )?;
        }

        written += batch;
        eprintln!(
            "  {:.0}%  ({} / {} objects)",
            written as f64 * 100.0 / n as f64,
            written,
            n,
        );
    }

    // Each array lives in its own segment so both the HPROF record length and
    // the element count stay within their 32-bit format fields. Write payload
    // in 1 MiB pieces; generating a 100 GiB file does not allocate 100 GiB.
    let zero = vec![0u8; MIB as usize];
    let mut payload_written = 0u64;
    for i in 0..array_count {
        let payload_len = (byte_array_bytes - payload_written).min(array_block_bytes);
        let segment_len = payload_len + 18; // tag + ID + stack + count + type
        write_seg_hdr(w, segment_len as u32)?;
        w.write_all(&[0x23])?; // TAG_GC_PRIM_ARRAY_DUMP
        w.write_all(&(array_base + i).to_be_bytes())?;
        w.write_all(&0u32.to_be_bytes())?; // stack serial
        w.write_all(&(payload_len as u32).to_be_bytes())?;
        w.write_all(&[8])?; // byte element type
        let mut left = payload_len;
        while left > 0 {
            let count = left.min(MIB) as usize;
            w.write_all(&zero[..count])?;
            left -= count as u64;
        }
        payload_written += payload_len;
        if (i + 1) % 64 == 0 || i + 1 == array_count {
            eprintln!(
                "  byte arrays: {} / {} MiB",
                payload_written / MIB,
                a.byte_array_mib
            );
        }
    }

    // ── HEAP_DUMP_END ─────────────────────────────────────────────────────────
    w.write_all(&[0x2C])?;
    w.write_all(&0u32.to_be_bytes())?;
    w.write_all(&0u32.to_be_bytes())?;

    Ok(())
}

// ── Sub-record size formulas ──────────────────────────────────────────────────

/// Byte size of one ClassDump GC sub-record (including tag byte).
fn class_dump_sub_record_size(total_fields: u32) -> u32 {
    // tag(1) + class_id(8) + stack_serial(4) + super_id(8)
    // + [class_loader, signers, prot_domain, reserved×2](5×8)
    // + instance_size(4) + cp_count(2) + static_count(2) + field_count(2)
    // + total_fields × (name_id(8) + type_tag(1))
    71 + total_fields * 9
}

/// Byte size of one InstanceDump GC sub-record (including tag byte).
fn instance_sub_record_size(obj_fields: u32, prim_fields: u32) -> u32 {
    // tag(1) + oid(8) + stack_serial(4) + class_id(8) + data_size(4)
    // + data (obj_fields×8 + prim_fields×4)
    25 + obj_fields * 8 + prim_fields * 4
}

// ── Write helpers ─────────────────────────────────────────────────────────────

fn write_utf8(w: &mut impl Write, id: u64, bytes: &[u8]) -> std::io::Result<()> {
    w.write_all(&[0x01])?;
    w.write_all(&0u32.to_be_bytes())?;
    w.write_all(&((8 + bytes.len()) as u32).to_be_bytes())?;
    w.write_all(&id.to_be_bytes())?;
    w.write_all(bytes)
}

fn write_load_class(
    w: &mut impl Write,
    serial: u32,
    class_id: u64,
    name_id: u64,
) -> std::io::Result<()> {
    w.write_all(&[0x02])?;
    w.write_all(&0u32.to_be_bytes())?;
    w.write_all(&24u32.to_be_bytes())?; // body = serial(4) + class_id(8) + stack(4) + name_id(8)
    w.write_all(&serial.to_be_bytes())?;
    w.write_all(&class_id.to_be_bytes())?;
    w.write_all(&0u32.to_be_bytes())?; // stack_serial
    w.write_all(&name_id.to_be_bytes())
}

fn write_seg_hdr(w: &mut impl Write, body_len: u32) -> std::io::Result<()> {
    w.write_all(&[0x1C])?; // HEAP_DUMP_SEGMENT
    w.write_all(&0u32.to_be_bytes())?;
    w.write_all(&body_len.to_be_bytes())
}

fn write_class_dump(
    w: &mut impl Write,
    class_id: u64,
    super_id: u64,
    instance_size: u32,
    obj_fields: u32,
    prim_fields: u32,
    field_name_ids: &[u64],
) -> std::io::Result<()> {
    w.write_all(&[0x20])?; // TAG_GC_CLASS_DUMP
    w.write_all(&class_id.to_be_bytes())?;
    w.write_all(&0u32.to_be_bytes())?; // stack_serial
    w.write_all(&super_id.to_be_bytes())?;
    w.write_all(&0u64.to_be_bytes())?; // class_loader_id
    w.write_all(&0u64.to_be_bytes())?; // signers_id
    w.write_all(&0u64.to_be_bytes())?; // protection_domain_id
    w.write_all(&0u64.to_be_bytes())?; // reserved_1
    w.write_all(&0u64.to_be_bytes())?; // reserved_2
    w.write_all(&instance_size.to_be_bytes())?;
    w.write_all(&0u16.to_be_bytes())?; // constant pool count = 0
    w.write_all(&0u16.to_be_bytes())?; // static fields count = 0
    w.write_all(&((obj_fields + prim_fields) as u16).to_be_bytes())?;
    for &name_id in field_name_ids.iter().take(obj_fields as usize) {
        w.write_all(&name_id.to_be_bytes())?;
        w.write_all(&[2u8])?; // Object
    }
    for &name_id in field_name_ids
        .iter()
        .skip(obj_fields as usize)
        .take(prim_fields as usize)
    {
        w.write_all(&name_id.to_be_bytes())?;
        w.write_all(&[10u8])?; // int
    }
    Ok(())
}

// Each argument maps to an independent HPROF field or generator parameter.
#[allow(clippy::too_many_arguments)]
fn write_instance_dump(
    w: &mut impl Write,
    oid: u64,
    class_id: u64,
    obj_fields: u32,
    prim_fields: u32,
    null_pct: u32,
    inst_base: u64,
    num_objects: u64,
    rng: &mut Rng,
) -> std::io::Result<()> {
    let data_size = obj_fields * 8 + prim_fields * 4;
    w.write_all(&[0x21])?; // TAG_GC_INSTANCE_DUMP
    w.write_all(&oid.to_be_bytes())?;
    w.write_all(&0u32.to_be_bytes())?; // stack_serial
    w.write_all(&class_id.to_be_bytes())?;
    w.write_all(&data_size.to_be_bytes())?;
    for _ in 0..obj_fields {
        let ref_id = if rng.range(100) < null_pct as u64 {
            0u64
        } else {
            inst_base + rng.range(num_objects)
        };
        w.write_all(&ref_id.to_be_bytes())?;
    }
    for _ in 0..prim_fields {
        w.write_all(&0u32.to_be_bytes())?;
    }
    Ok(())
}

fn write_gc_root_jni_global(w: &mut impl Write, oid: u64, jni_ref: u64) -> std::io::Result<()> {
    w.write_all(&[0x01])?; // TAG_GC_ROOT_JNI_GLOBAL
    w.write_all(&oid.to_be_bytes())?;
    w.write_all(&jni_ref.to_be_bytes())
}

// ── xorshift64 RNG ────────────────────────────────────────────────────────────

struct Rng {
    state: u64,
}

impl Rng {
    fn new(seed: u64) -> Self {
        let mut s = Self {
            state: if seed == 0 { 1 } else { seed },
        };
        // Warm up to avoid weak initial outputs.
        s.next();
        s.next();
        s
    }

    #[inline]
    fn next(&mut self) -> u64 {
        self.state ^= self.state << 13;
        self.state ^= self.state >> 7;
        self.state ^= self.state << 17;
        self.state
    }

    /// Uniformly random value in `[0, n)`. Returns 0 if n == 0.
    #[inline]
    fn range(&mut self, n: u64) -> u64 {
        if n == 0 { 0 } else { self.next() % n }
    }
}
