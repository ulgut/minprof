// SPDX-License-Identifier: Apache-2.0
//
// Adapted from hprof-slurp <https://github.com/agourlay/hprof-slurp>
// Copyright (c) Arnaud Gourlay and hprof-slurp contributors.
// Licensed under the Apache License, Version 2.0.

use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;
use std::thread;

use anyhow::{Context, Result, anyhow};
use crossbeam_channel::bounded;

use crate::parser::file_header_parser::{FileHeader, parse_file_header};
use crate::parser::record::Record;
use crate::parser::record_parser::HprofRecordParser;

/// Size of each file read — large enough to keep the parser fed, small enough
/// to bound RAM. Three of these live in flight at once (POOL_DEPTH).
pub const READ_BUFFER_SIZE: usize = 64 * 1024 * 1024; // 64MB

/// Max chunks buffered between adjacent pipeline stages.
const POOL_DEPTH: usize = 3;

/// Implement this to consume records produced by the pipeline.
/// Called on the main thread sequentially — no synchronization needed.
pub trait RecordVisitor {
    fn on_record(&mut self, record: Record);

    /// Called once after all records have been delivered.
    fn finish(&mut self) {}
}

/// Parse a HPROF file at `path`, delivering every record to `visitor`.
///
/// Internally runs a 3-stage pipeline:
///   reader thread → parser thread → main thread (visitor)
///
/// Peak RAM: O(READ_BUFFER_SIZE * POOL_DEPTH) for I/O buffers, plus whatever
/// the visitor accumulates. The parser working buffer grows only as large as
/// the largest single HPROF record.
pub fn process(path: &Path, visitor: &mut dyn RecordVisitor) -> Result<()> {
    process_impl(path, visitor, false)
}

/// Like [`process`] but enables data mode: `InstanceDump` and `ObjectArrayDump`
/// records carry their raw bytes / decoded element IDs. Used by pass 2.
pub fn process_with_data(path: &Path, visitor: &mut dyn RecordVisitor) -> Result<()> {
    process_impl(path, visitor, true)
}

fn process_impl(path: &Path, visitor: &mut dyn RecordVisitor, include_data: bool) -> Result<()> {
    // -- File header ----------------------------------------------------------
    // Parse the header on the main thread so we know id_size before spawning
    // the parser thread.
    let (header, header_bytes_consumed) = read_header(path)?;
    let id_size = header.id_size;
    // Own the path so it can be moved into the reader thread.
    let path = path.to_path_buf();

    // -- Channels -------------------------------------------------------------
    // reader → parser: filled byte buffers
    let (send_chunk, recv_chunk) = bounded::<Vec<u8>>(POOL_DEPTH);
    // parser → reader: drained (empty) byte buffers for reuse
    let (send_pooled_buf, recv_pooled_buf) = bounded::<Vec<u8>>(POOL_DEPTH);
    // parser → main: batches of parsed records
    let (send_records, recv_records) = bounded::<Vec<Record>>(POOL_DEPTH);
    // main → parser: cleared record vecs for reuse
    let (send_pooled_vec, recv_pooled_vec) = bounded::<Vec<Record>>(POOL_DEPTH);

    // Pre-fill pools so threads don't stall waiting for the first buffer.
    for _ in 0..POOL_DEPTH {
        send_pooled_buf
            .send(Vec::with_capacity(READ_BUFFER_SIZE))
            .unwrap();
        send_pooled_vec.send(Vec::new()).unwrap();
    }

    // -- Reader thread --------------------------------------------------------
    let reader = thread::Builder::new()
        .name("hprof-reader".into())
        .spawn(move || -> Result<()> {
            let mut file = File::open(path).context("open hprof file")?;
            file.seek(SeekFrom::Start(header_bytes_consumed as u64))
                .context("seek past header")?;

            loop {
                // Grab a pooled buffer, fill it.
                let mut buf = recv_pooled_buf
                    .recv()
                    .expect("pool channel closed prematurely");
                buf.resize(READ_BUFFER_SIZE, 0);

                let n = file.read(&mut buf).context("read hprof chunk")?;
                if n == 0 {
                    break; // EOF
                }
                buf.truncate(n);
                send_chunk.send(buf).expect("parser channel closed");
            }
            // Dropping send_chunk signals EOF to the parser.
            Ok(())
        })?;

    // -- Parser thread --------------------------------------------------------
    let parser = thread::Builder::new()
        .name("hprof-parser".into())
        .spawn(move || {
            let mut parser = if include_data {
                HprofRecordParser::with_data(id_size)
            } else {
                HprofRecordParser::new(id_size)
            };
            // Working buffer: accumulates data across chunk boundaries so that
            // records that span two chunks are handled correctly.
            let mut work_buf: Vec<u8> = Vec::new();

            while let Ok(mut chunk) = recv_chunk.recv() {
                // Move chunk data into the working buffer, then return the
                // now-empty chunk allocation to the reader pool.
                work_buf.append(&mut chunk);
                send_pooled_buf
                    .send(chunk)
                    .expect("reader pool channel closed");

                // Get a record vec to fill for this iteration.
                let mut records = recv_pooled_vec
                    .recv()
                    .expect("consumer pool channel closed");

                match parser.parse_streaming(&work_buf, &mut records) {
                    Ok((rest, ())) => {
                        // Drain consumed bytes from the front of the working buffer.
                        let consumed = work_buf.len() - rest.len();
                        work_buf.drain(0..consumed);
                    }
                    Err(nom::Err::Incomplete(_)) => {
                        // The buffer didn't contain even a single complete record.
                        // More data is needed — leave work_buf untouched.
                    }
                    Err(e) => panic!("HPROF parse error: {e:?}"),
                }

                // Send the batch (may be empty if we got Incomplete).
                send_records
                    .send(records)
                    .expect("consumer records channel closed");
            }
            // Dropping send_records signals EOF to the main thread.
        })?;

    // -- Main thread: consume -------------------------------------------------
    for mut records in recv_records {
        for record in records.drain(..) {
            visitor.on_record(record);
        }
        // Return the now-empty vec to the parser pool.
        send_pooled_vec
            .send(records)
            .expect("parser pool channel closed");
    }

    visitor.finish();

    reader
        .join()
        .expect("reader thread panicked")
        .context("reader thread error")?;
    parser.join().expect("parser thread panicked");

    Ok(())
}

// ---------------------------------------------------------------------------
// Header reading
// ---------------------------------------------------------------------------

/// Read and parse the HPROF file header, returning it along with the number
/// of bytes consumed (so the reader thread can seek past them).
pub fn read_header(path: &Path) -> Result<(FileHeader, usize)> {
    // The header is: format_string\0 (≤ 20 bytes) + u32 id_size + u64 timestamp
    // = at most ~33 bytes. Read 256 to be safe.
    let mut file = File::open(path).context("open hprof file for header")?;
    let mut buf = vec![0u8; 256];
    let n = file.read(&mut buf).context("read hprof header bytes")?;
    buf.truncate(n);

    let (rest, header) = parse_file_header(&buf)
        .map_err(|e| anyhow!("failed to parse HPROF file header: {e:?}"))?;

    let consumed = buf.len() - rest.len();
    Ok((header, consumed))
}
