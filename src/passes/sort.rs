//! Generic external sorter for fixed-size binary records.
//!
//! `RecordSorter<N>` accumulates `[u8; N]` records in a sort buffer (40% of
//! RAM), sorts and flushes full chunks to disk on a background thread (so the
//! sort + write overlaps with continued production on the caller thread), and
//! merges the chunks in a final k-way pass.  The sort key is a
//! `fn(&[u8; N]) -> (u64, u64)`, so the same struct sorts records of any layout
//! by any two-part key.
//!
//! Two optional behaviours, selected at construction, specialise it for every
//! pass without per-pass sorter types:
//!   * **dedup** — drop consecutive equal records (used by the edge sort).
//!   * **fixup** — a closure applied to each record as it is written in
//!     [`RecordSorter::finish_with_fixup`] (used by pass 1 to patch instance
//!     shallow sizes and emit the `shallow_sizes.bin` sidecar).

use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::thread;

use anyhow::{Context, Result};
use rayon::slice::ParallelSliceMut;

use crate::passes::{IO_BUF_SIZE, MAX_MERGE_FAN_IN};

pub struct RecordSorter<const N: usize> {
    output_dir: PathBuf,
    prefix: String,
    chunk_paths: Vec<PathBuf>,
    current: Vec<[u8; N]>,
    records_per_chunk: usize,
    /// Monotonic chunk counter — used for filenames so a background flush that
    /// hasn't been collected yet still gets a unique name.
    chunk_count: usize,
    key_fn: fn(&[u8; N]) -> (u64, u64),
    dedup: bool,
    /// In-flight background sort+write task. At most one pending; collected
    /// (joined) before the next flush and in `finish`.
    pending_flush: Option<thread::JoinHandle<Result<PathBuf>>>,
}

impl<const N: usize> RecordSorter<N> {
    pub fn new(output_dir: PathBuf, prefix: &str, key_fn: fn(&[u8; N]) -> (u64, u64)) -> Self {
        let chunk_bytes = crate::passes::sort_chunk_bytes();
        let records_per_chunk = chunk_bytes / N;
        eprintln!(
            "  sort buffer [{prefix}]: {:.1} GiB ({} records/chunk)",
            chunk_bytes as f64 / (1u64 << 30) as f64,
            records_per_chunk,
        );
        Self {
            output_dir,
            prefix: prefix.to_string(),
            chunk_paths: Vec::new(),
            current: Vec::with_capacity(records_per_chunk),
            records_per_chunk,
            chunk_count: 0,
            key_fn,
            dedup: false,
            pending_flush: None,
        }
    }

    /// Enable dropping of consecutive equal records (both within a sorted chunk
    /// and across the final merge).
    pub fn dedup(mut self) -> Self {
        self.dedup = true;
        self
    }

    pub fn push(&mut self, record: [u8; N]) -> Result<()> {
        self.current.push(record);
        if self.current.len() >= self.records_per_chunk {
            self.flush_chunk()?;
        }
        Ok(())
    }

    /// Join the in-flight sort+write thread (if any) and record its output path.
    fn collect_pending(&mut self) -> Result<()> {
        if let Some(handle) = self.pending_flush.take() {
            let path = handle.join().expect("sort-flush thread panicked")?;
            self.chunk_paths.push(path);
        }
        Ok(())
    }

    /// Sort the current buffer and write it to disk on a background thread,
    /// overlapping the write with continued production on the caller thread.
    ///
    /// The old buffer is moved into the thread (its pages are freed on join); a
    /// fresh demand-paged buffer is allocated immediately. `collect_pending` is
    /// called first, so at most one old buffer is ever live.
    fn flush_chunk(&mut self) -> Result<()> {
        if self.current.is_empty() {
            return Ok(());
        }
        self.collect_pending()?;

        let chunk_idx = self.chunk_count;
        self.chunk_count += 1;
        let path = self
            .output_dir
            .join(format!("{}_chunk_{chunk_idx}.bin", self.prefix));
        let prefix = self.prefix.clone();
        let key_fn = self.key_fn;
        let dedup = self.dedup;

        let to_sort =
            std::mem::replace(&mut self.current, Vec::with_capacity(self.records_per_chunk));

        let handle = thread::Builder::new()
            .name(format!("{prefix}-flush-{chunk_idx}"))
            .spawn(move || -> Result<PathBuf> {
                let mut buf = to_sort;
                buf.par_sort_unstable_by_key(|e| key_fn(e));
                if dedup {
                    buf.dedup();
                }
                let mut w = BufWriter::with_capacity(
                    IO_BUF_SIZE,
                    File::create(&path).context("create sort chunk")?,
                );
                // Safety: [u8; N] is a plain byte array (alignment 1, no
                // padding); Vec<[u8; N]> stores records back-to-back.
                let bytes = unsafe {
                    std::slice::from_raw_parts(buf.as_ptr().cast::<u8>(), buf.len() * N)
                };
                w.write_all(bytes)?;
                w.flush()?;
                eprintln!("  [{prefix}] flushed chunk {}", chunk_idx + 1);
                Ok(path)
            })?;

        self.pending_flush = Some(handle);
        Ok(())
    }

    /// Merge all chunks into a single sorted file at `output_path`.
    pub fn finish(self, output_path: &Path) -> Result<u64> {
        self.finish_with_fixup(output_path, |_| {})
    }

    /// Merge all chunks into a single sorted file at `output_path`, applying
    /// `fixup` to every record immediately before it is written.
    pub fn finish_with_fixup<F>(mut self, output_path: &Path, mut fixup: F) -> Result<u64>
    where
        F: FnMut(&mut [u8; N]),
    {
        self.collect_pending()?;

        // Fast path: everything still fits in one buffer — sort in memory and
        // write directly, no chunk files.
        if self.chunk_paths.is_empty() {
            if self.current.is_empty() {
                File::create(output_path).context("create empty sort output")?;
                return Ok(0);
            }
            let key_fn = self.key_fn;
            self.current.par_sort_unstable_by_key(|e| key_fn(e));
            if self.dedup {
                self.current.dedup();
            }
            let count = self.current.len() as u64;
            let mut w = BufWriter::with_capacity(
                IO_BUF_SIZE,
                File::create(output_path).context("create sort output")?,
            );
            for mut rec in self.current.drain(..) {
                fixup(&mut rec);
                w.write_all(&rec)?;
            }
            w.flush()?;
            return Ok(count);
        }

        // Flush any remaining in-memory records, then collect that flush.
        self.flush_chunk()?;
        self.collect_pending()?;
        self.current = Vec::new(); // release sort buffer before merge

        let chunks = std::mem::take(&mut self.chunk_paths);

        match chunks.len() {
            0 => unreachable!("chunk_paths non-empty above"),
            n if n <= MAX_MERGE_FAN_IN => {
                eprintln!("  [{}] merging {} chunks…", self.prefix, n);
                merge_sorted_chunks::<N, _>(&chunks, output_path, self.key_fn, self.dedup, fixup)?;
                for p in &chunks {
                    let _ = std::fs::remove_file(p);
                }
            }
            n => {
                // Two-level merge to cap peak file-descriptor count.
                // Intermediates are written without fixup; fixup is applied only
                // during the final merge pass.
                let group_size = MAX_MERGE_FAN_IN;
                let num_groups = n.div_ceil(group_size);
                eprintln!(
                    "  [{}] two-level merge: {} chunks → {} groups…",
                    self.prefix, n, num_groups
                );
                let mut intermediates: Vec<PathBuf> = Vec::with_capacity(num_groups);
                for (g, group) in chunks.chunks(group_size).enumerate() {
                    let inter = self
                        .output_dir
                        .join(format!("{}_inter_{g}.bin", self.prefix));
                    eprintln!(
                        "    merging group {}/{} ({} chunks)…",
                        g + 1,
                        num_groups,
                        group.len()
                    );
                    merge_sorted_chunks::<N, _>(group, &inter, self.key_fn, self.dedup, |_| {})?;
                    for p in group {
                        let _ = std::fs::remove_file(p);
                    }
                    intermediates.push(inter);
                }
                eprintln!(
                    "  [{}] final merge of {} groups…",
                    self.prefix,
                    intermediates.len()
                );
                merge_sorted_chunks::<N, _>(
                    &intermediates,
                    output_path,
                    self.key_fn,
                    self.dedup,
                    fixup,
                )?;
                for p in &intermediates {
                    let _ = std::fs::remove_file(p);
                }
            }
        }

        let count = std::fs::metadata(output_path)?.len() / N as u64;
        Ok(count)
    }
}

/// Clean up chunk files if the sorter is dropped before `finish` (e.g. on panic).
impl<const N: usize> Drop for RecordSorter<N> {
    fn drop(&mut self) {
        if let Some(handle) = self.pending_flush.take() {
            if let Ok(Ok(path)) = handle.join() {
                self.chunk_paths.push(path);
            }
        }
        for p in &self.chunk_paths {
            let _ = std::fs::remove_file(p);
        }
    }
}

fn merge_sorted_chunks<const N: usize, F>(
    chunk_paths: &[PathBuf],
    output_path: &Path,
    key_fn: fn(&[u8; N]) -> (u64, u64),
    dedup: bool,
    mut fixup: F,
) -> Result<()>
where
    F: FnMut(&mut [u8; N]),
{
    let per_reader_buf = (IO_BUF_SIZE / chunk_paths.len().max(1)).max(256 * 1024);
    let mut readers: Vec<BufReader<File>> = chunk_paths
        .iter()
        .map(|p| {
            Ok(BufReader::with_capacity(
                per_reader_buf,
                File::open(p).context("open sort chunk")?,
            ))
        })
        .collect::<Result<_>>()?;

    let mut heap: BinaryHeap<Reverse<(u64, u64, usize)>> = BinaryHeap::new();
    let mut peek: Vec<Option<[u8; N]>> = vec![None; readers.len()];

    for (i, r) in readers.iter_mut().enumerate() {
        if let Some(rec) = read_record::<N>(r)? {
            let (k0, k1) = key_fn(&rec);
            heap.push(Reverse((k0, k1, i)));
            peek[i] = Some(rec);
        }
    }

    let mut w = BufWriter::with_capacity(
        IO_BUF_SIZE,
        File::create(output_path).context("create merged sort output")?,
    );
    let mut last: Option<[u8; N]> = None;
    while let Some(Reverse((_, _, idx))) = heap.pop() {
        let mut rec = peek[idx].take().unwrap();
        if !(dedup && last.as_ref() == Some(&rec)) {
            if dedup {
                last = Some(rec);
            }
            fixup(&mut rec);
            w.write_all(&rec)?;
        }
        if let Some(next) = read_record::<N>(&mut readers[idx])? {
            let (k0, k1) = key_fn(&next);
            heap.push(Reverse((k0, k1, idx)));
            peek[idx] = Some(next);
        }
    }
    w.flush()?;
    Ok(())
}

fn read_record<const N: usize>(r: &mut impl Read) -> Result<Option<[u8; N]>> {
    let mut buf = [0u8; N];
    match r.read_exact(&mut buf) {
        Ok(()) => Ok(Some(buf)),
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => Ok(None),
        Err(e) => Err(e).context("read sort record"),
    }
}
