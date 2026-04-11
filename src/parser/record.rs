// SPDX-License-Identifier: Apache-2.0
//
// Adapted from hprof-slurp <https://github.com/agourlay/hprof-slurp>
// Copyright (c) Arnaud Gourlay and hprof-slurp contributors.
// Licensed under the Apache License, Version 2.0.

use crate::parser::gc_record::GcRecord;

#[derive(Debug)]
pub struct LoadClassData {
    pub serial_number: u32,
    pub class_object_id: u64,
    pub class_name_id: u64,
}

#[derive(Debug)]
pub enum Record {
    Utf8String {
        id: u64,
        str: Box<str>,
    },
    LoadClass(LoadClassData),
    HeapDumpStart,
    HeapDumpEnd,
    /// A single sub-record parsed out of a HEAP_DUMP / HEAP_DUMP_SEGMENT body.
    GcSegment(GcRecord),
    /// Any top-level record tag we don't need (STACK_FRAME, etc.) — skipped.
    Ignored,
}
