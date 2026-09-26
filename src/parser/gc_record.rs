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

#[derive(Debug, Clone)]
pub struct FieldInfo {
    /// String ID of the field name (resolvable from the string table).
    /// Parsed from the binary format; not yet used but retained for future
    /// field-name display in path-to-GC-root output.
    #[allow(dead_code)]
    pub name_id: u64,
    pub field_type: FieldType,
}
