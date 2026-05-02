//! Generic external sorter for fixed-size binary records.
//!
//! `RecordSorter<N>` accumulates `[u8; N]` records in a sort buffer (40% of
//! RAM), flushes sorted chunks to disk, and merges them in a final pass.
//! The sort key is provided as a `fn(&[u8; N]) -> (u64, u64)` so the same
//! struct can sort records of any layout by any two-part key.

use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use rayon::slice::ParallelSliceMut;

use crate::passes::{IO_BUF_SIZE, MAX_MERGE_FAN_IN};

pub struct RecordSorter<const N: usize> {
    output_dir: PathBuf,
    prefix: String,
    chunk_paths: Vec<PathBuf>,
    current: Vec<[u8; N]>,
    records_per_chunk: usize,
    key_fn: fn(&[u8; N]) -> (u64, u64),
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
            key_fn,
        }
    }

    pub fn push(&mut self, record: [u8; N]) -> Result<()> {
        self.current.push(record);
        if self.current.len() >= self.records_per_chunk {
            self.flush_chunk()?;
        }
        Ok(())
    }

    fn flush_chunk(&mut self) -> Result<()> {
        if self.current.is_empty() {
            return Ok(());
        }
        let key_fn = self.key_fn;
        self.current.par_sort_unstable_by_key(|e| key_fn(e));

        let idx = self.chunk_paths.len();
        let path = self
            .output_dir
            .join(format!("{}_chunk_{idx}.bin", self.prefix));
        let mut w = BufWriter::with_capacity(
            IO_BUF_SIZE,
            File::create(&path).context("create sort chunk")?,
        );
        // Write the entire sort buffer as one contiguous byte slice.
        // Safety: [u8; N] is a plain byte array with alignment 1 and no interior
        // padding.  Vec<[u8; N]> stores records back-to-back, so the backing
        // allocation is a valid &[u8] of exactly len * N bytes.
        let bytes = unsafe {
            std::slice::from_raw_parts(self.current.as_ptr().cast::<u8>(), self.current.len() * N)
        };
        w.write_all(bytes)?;
        self.current.clear();
        w.flush()?;
        self.chunk_paths.push(path);
        eprintln!(
            "  [{}] flushed chunk {}",
            self.prefix,
            self.chunk_paths.len()
        );
        Ok(())
    }

    pub fn finish(mut self, output_path: &Path) -> Result<u64> {
        // Fast path: everything fit in one buffer — sort in-memory, write directly.
        if self.chunk_paths.is_empty() {
            if self.current.is_empty() {
                File::create(output_path).context("create empty sort output")?;
                return Ok(0);
            }
            let key_fn = self.key_fn;
            self.current.par_sort_unstable_by_key(|e| key_fn(e));
            let count = self.current.len() as u64;
            let mut w = BufWriter::with_capacity(
                IO_BUF_SIZE,
                File::create(output_path).context("create sort output")?,
            );
            let bytes = unsafe {
                std::slice::from_raw_parts(
                    self.current.as_ptr().cast::<u8>(),
                    self.current.len() * N,
                )
            };
            w.write_all(bytes)?;
            w.flush()?;
            return Ok(count);
        }

        self.flush_chunk()?;
        self.current = Vec::new(); // release sort buffer before merge

        let chunks = std::mem::take(&mut self.chunk_paths);

        match chunks.len() {
            0 => {
                File::create(output_path).context("create empty sort output")?;
                Ok(0)
            }
            n if n <= MAX_MERGE_FAN_IN => {
                eprintln!("  [{}] merging {} chunks…", self.prefix, n);
                merge_sorted_chunks::<N>(&chunks, output_path, self.key_fn)?;
                for p in &chunks {
                    let _ = std::fs::remove_file(p);
                }
                let count = std::fs::metadata(output_path)?.len() / N as u64;
                Ok(count)
            }
            n => {
                let group_size = MAX_MERGE_FAN_IN;
                let num_groups = (n + group_size - 1) / group_size;
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
                    merge_sorted_chunks::<N>(group, &inter, self.key_fn)?;
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
                merge_sorted_chunks::<N>(&intermediates, output_path, self.key_fn)?;
                for p in &intermediates {
                    let _ = std::fs::remove_file(p);
                }
                let count = std::fs::metadata(output_path)?.len() / N as u64;
                Ok(count)
            }
        }
    }
}

impl<const N: usize> Drop for RecordSorter<N> {
    fn drop(&mut self) {
        for p in &self.chunk_paths {
            let _ = std::fs::remove_file(p);
        }
    }
}

fn merge_sorted_chunks<const N: usize>(
    chunk_paths: &[PathBuf],
    output_path: &Path,
    key_fn: fn(&[u8; N]) -> (u64, u64),
) -> Result<()> {
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
    while let Some(Reverse((_, _, idx))) = heap.pop() {
        let rec = peek[idx].take().unwrap();
        w.write_all(&rec)?;
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
