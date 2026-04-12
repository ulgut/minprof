// SPDX-License-Identifier: Apache-2.0
//
// Adapted from hprof-slurp <https://github.com/agourlay/hprof-slurp>
// Copyright (c) Arnaud Gourlay and hprof-slurp contributors.
// Licensed under the Apache License, Version 2.0.

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum FieldType {
    Object = 2,
    Bool = 4,
    Char = 5,
    Float = 6,
    Double = 7,
    Byte = 8,
    Short = 9,
    Int = 10,
    Long = 11,
}

impl FieldType {
    pub fn from_value(v: u8) -> Self {
        match v {
            2 => Self::Object,
            4 => Self::Bool,
            5 => Self::Char,
            6 => Self::Float,
            7 => Self::Double,
            8 => Self::Byte,
            9 => Self::Short,
            10 => Self::Int,
            11 => Self::Long,
            x => panic!("unknown FieldType value: {x}"),
        }
    }

    /// Size of this field type in bytes, given the id_size for Object fields.
    pub fn byte_size(self, id_size: u32) -> u32 {
        match self {
            Self::Object => id_size,
            Self::Bool | Self::Byte => 1,
            Self::Char | Self::Short => 2,
            Self::Float | Self::Int => 4,
            Self::Double | Self::Long => 8,
        }
    }
}

#[derive(Debug)]
pub struct FieldInfo {
    /// String ID of the field name (resolvable from the string table).
    /// Parsed from the binary format; not yet used but retained for future
    /// field-name display in path-to-GC-root output.
    #[allow(dead_code)]
    pub name_id: u64,
    pub field_type: FieldType,
}

#[derive(Debug)]
pub enum FieldValue {
    // Primitive variants are parsed to correctly advance the byte cursor.
    // Only Object references are used in analysis; the others are kept for
    // completeness and potential future use (e.g. string content extraction).
    #[allow(dead_code)] Bool(bool),
    #[allow(dead_code)] Byte(i8),
    #[allow(dead_code)] Char(u16),
    #[allow(dead_code)] Short(i16),
    #[allow(dead_code)] Int(i32),
    #[allow(dead_code)] Long(i64),
    #[allow(dead_code)] Float(f32),
    #[allow(dead_code)] Double(f64),
    Object(u64),
}

#[derive(Debug)]
pub struct ClassDumpFields {
    pub class_object_id: u64,
    pub super_class_object_id: u64,
    pub instance_size: u32,
    pub instance_fields: Vec<FieldInfo>,
    // Static fields are included for completeness but not used in core analysis.
    pub static_fields: Vec<(FieldInfo, FieldValue)>,
}

/// A sub-record within a HEAP_DUMP or HEAP_DUMP_SEGMENT block.
///
/// Auxiliary root fields (thread_serial, frame_num, etc.) are parsed from the
/// HPROF binary format and retained for potential future use (e.g. attributing
/// retained memory to specific threads). They are not currently read by the
/// analysis passes.
#[allow(dead_code)]
#[derive(Debug)]
pub enum GcRecord {
    RootUnknown       { object_id: u64 },
    RootJniGlobal     { object_id: u64, jni_global_ref_id: u64 },
    RootJniLocal      { object_id: u64, thread_serial: u32, frame_num: u32 },
    RootJavaFrame     { object_id: u64, thread_serial: u32, frame_num: u32 },
    RootNativeStack   { object_id: u64, thread_serial: u32 },
    RootStickyClass   { object_id: u64 },
    RootThreadBlock   { object_id: u64, thread_serial: u32 },
    RootMonitorUsed   { object_id: u64 },
    RootThreadObject  { object_id: u64, thread_serial: u32, stack_serial: u32 },

    ClassDump(Box<ClassDumpFields>),

    InstanceDump {
        object_id: u64,
        class_id:  u64,
        data_size: u32,
        /// Raw instance field bytes, in HPROF big-endian layout.
        /// Empty in pass 1 (skip mode); populated in pass 2 (data mode).
        raw_data:  Vec<u8>,
    },

    ObjectArrayDump {
        object_id:        u64,
        num_elements:     u32,
        element_class_id: u64,
        /// Element object IDs (big-endian decoded).
        /// Empty in pass 1 (skip mode); populated in pass 2 (data mode).
        elements: Vec<u64>,
    },

    PrimitiveArrayDump {
        object_id:    u64,
        num_elements: u32,
        element_type: FieldType,
    },
}

