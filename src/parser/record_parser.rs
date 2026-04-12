// SPDX-License-Identifier: Apache-2.0
//
// Adapted from hprof-slurp <https://github.com/agourlay/hprof-slurp>
// Copyright (c) Arnaud Gourlay and hprof-slurp contributors.
// Licensed under the Apache License, Version 2.0.

use crate::parser::gc_record::{
    ClassDumpFields, FieldInfo, FieldType, FieldValue, GcRecord,
};
use crate::parser::primitive_parsers::{
    parse_f32, parse_f64, parse_i8, parse_i16, parse_i32, parse_i64, parse_u8, parse_u16,
    parse_u32, parse_u64,
};
use crate::parser::record::{LoadClassData, Record};
use nom::error::{ErrorKind, ParseError};
use nom::{IResult, Parser, bytes};
use nom::combinator::map;

// Top-level record tags
const TAG_STRING: u8 = 0x01;
const TAG_LOAD_CLASS: u8 = 0x02;
const TAG_HEAP_DUMP: u8 = 0x0C;
const TAG_HEAP_DUMP_SEGMENT: u8 = 0x1C;
const TAG_HEAP_DUMP_END: u8 = 0x2C;

// GC sub-record tags
const TAG_GC_ROOT_UNKNOWN: u8 = 0xFF;
const TAG_GC_ROOT_JNI_GLOBAL: u8 = 0x01;
const TAG_GC_ROOT_JNI_LOCAL: u8 = 0x02;
const TAG_GC_ROOT_JAVA_FRAME: u8 = 0x03;
const TAG_GC_ROOT_NATIVE_STACK: u8 = 0x04;
const TAG_GC_ROOT_STICKY_CLASS: u8 = 0x05;
const TAG_GC_ROOT_THREAD_BLOCK: u8 = 0x06;
const TAG_GC_ROOT_MONITOR_USED: u8 = 0x07;
const TAG_GC_ROOT_THREAD_OBJ: u8 = 0x08;
const TAG_GC_CLASS_DUMP: u8 = 0x20;
const TAG_GC_INSTANCE_DUMP: u8 = 0x21;
const TAG_GC_OBJ_ARRAY_DUMP: u8 = 0x22;
const TAG_GC_PRIM_ARRAY_DUMP: u8 = 0x23;

pub struct HprofRecordParser {
    id_size: u32,
    /// When true, InstanceDump and ObjectArrayDump carry their raw bytes /
    /// decoded element IDs. Set this for pass 2. False in pass 1.
    include_data: bool,
    /// Bytes remaining in the current HEAP_DUMP / HEAP_DUMP_SEGMENT body.
    /// Zero means we are between top-level records.
    heap_dump_remaining_len: u32,
}

impl HprofRecordParser {
    pub const fn new(id_size: u32) -> Self {
        Self {
            id_size,
            include_data: false,
            heap_dump_remaining_len: 0,
        }
    }

    pub const fn with_data(id_size: u32) -> Self {
        Self {
            id_size,
            include_data: true,
            heap_dump_remaining_len: 0,
        }
    }

    pub fn parse_hprof_record(&mut self) -> impl FnMut(&[u8]) -> IResult<&[u8], Record> + '_ {
        |i| {
            if self.heap_dump_remaining_len == 0 {
                let (r, tag) = parse_u8(i)?;
                match tag {
                    TAG_STRING => parse_utf8_string(self.id_size, r),
                    TAG_LOAD_CLASS => parse_load_class(self.id_size, r),
                    TAG_HEAP_DUMP | TAG_HEAP_DUMP_SEGMENT => {
                        let (r, (_time, length)) = parse_record_header(r)?;
                        self.heap_dump_remaining_len = length;
                        Ok((r, Record::HeapDumpStart))
                    }
                    TAG_HEAP_DUMP_END => {
                        let (r, _) = parse_record_header(r)?;
                        Ok((r, Record::HeapDumpEnd))
                    }
                    // All other top-level tags: read header, skip body.
                    _ => {
                        let (r, (_time, length)) = parse_record_header(r)?;
                        let (r, _) = bytes::streaming::take(length as u64)(r)?;
                        Ok((r, Record::Ignored))
                    }
                }
            } else {
                let (r, gc) = parse_gc_record(self.id_size, self.include_data, i)?;
                let consumed = i.len() - r.len();
                self.heap_dump_remaining_len =
                    self.heap_dump_remaining_len.saturating_sub(consumed as u32);
                Ok((r, Record::GcSegment(gc)))
            }
        }
    }

    pub fn parse_streaming<'a>(
        &mut self,
        i: &'a [u8],
        pooled_vec: &mut Vec<Record>,
    ) -> IResult<&'a [u8], ()> {
        lazy_many1(self.parse_hprof_record(), pooled_vec)(i)
    }
}

// ---------------------------------------------------------------------------
// Primitives
// ---------------------------------------------------------------------------

/// Parse a top-level record header: (time_offset_ms: u32, body_length: u32).
fn parse_record_header(i: &[u8]) -> IResult<&[u8], (u32, u32)> {
    (parse_u32, parse_u32).parse(i)
}

/// Parse an object ID — 4 or 8 bytes depending on id_size from the file header.
/// Always returned as u64 for uniform handling.
fn parse_id(id_size: u32, i: &[u8]) -> IResult<&[u8], u64> {
    match id_size {
        4 => map(parse_u32, |v| v as u64).parse(i),
        8 => parse_u64(i),
        _ => panic!("unsupported id_size: {id_size}"),
    }
}

fn parse_field_type(i: &[u8]) -> IResult<&[u8], FieldType> {
    map(parse_u8, FieldType::from_value).parse(i)
}

fn parse_field_value(id_size: u32, ty: FieldType, i: &[u8]) -> IResult<&[u8], FieldValue> {
    match ty {
        FieldType::Object => map(|i| parse_id(id_size, i), FieldValue::Object).parse(i),
        FieldType::Bool   => map(parse_u8,  |b| FieldValue::Bool(b != 0)).parse(i),
        FieldType::Char   => map(parse_u16, FieldValue::Char).parse(i),
        FieldType::Float  => map(parse_f32, FieldValue::Float).parse(i),
        FieldType::Double => map(parse_f64, FieldValue::Double).parse(i),
        FieldType::Byte   => map(parse_i8,  FieldValue::Byte).parse(i),
        FieldType::Short  => map(parse_i16, FieldValue::Short).parse(i),
        FieldType::Int    => map(parse_i32, FieldValue::Int).parse(i),
        FieldType::Long   => map(parse_i64, FieldValue::Long).parse(i),
    }
}

/// Skip the raw bytes of a primitive array without parsing element values.
fn skip_primitive_array(element_type: FieldType, num_elements: u32, i: &[u8]) -> IResult<&[u8], ()> {
    let n = num_elements as u64;
    let byte_count = match element_type {
        FieldType::Object                               => panic!("Object type in primitive array"),
        FieldType::Bool | FieldType::Byte               => n,
        FieldType::Char | FieldType::Short              => n * 2,
        FieldType::Float | FieldType::Int               => n * 4,
        FieldType::Double | FieldType::Long             => n * 8,
    };
    map(bytes::streaming::take(byte_count), |_| ()).parse(i)
}

// ---------------------------------------------------------------------------
// GC sub-record parsers
// ---------------------------------------------------------------------------

fn parse_gc_record(id_size: u32, include_data: bool, i: &[u8]) -> IResult<&[u8], GcRecord> {
    let (r, tag) = parse_u8(i)?;
    match tag {
        TAG_GC_ROOT_UNKNOWN => {
            let (r, object_id) = parse_id(id_size, r)?;
            Ok((r, GcRecord::RootUnknown { object_id }))
        }
        TAG_GC_ROOT_JNI_GLOBAL => {
            let (r, object_id)        = parse_id(id_size, r)?;
            let (r, jni_global_ref_id) = parse_id(id_size, r)?;
            Ok((r, GcRecord::RootJniGlobal { object_id, jni_global_ref_id }))
        }
        TAG_GC_ROOT_JNI_LOCAL => {
            let (r, object_id)    = parse_id(id_size, r)?;
            let (r, thread_serial) = parse_u32(r)?;
            let (r, frame_num)    = parse_u32(r)?;
            Ok((r, GcRecord::RootJniLocal { object_id, thread_serial, frame_num }))
        }
        TAG_GC_ROOT_JAVA_FRAME => {
            let (r, object_id)    = parse_id(id_size, r)?;
            let (r, thread_serial) = parse_u32(r)?;
            let (r, frame_num)    = parse_u32(r)?;
            Ok((r, GcRecord::RootJavaFrame { object_id, thread_serial, frame_num }))
        }
        TAG_GC_ROOT_NATIVE_STACK => {
            let (r, object_id)    = parse_id(id_size, r)?;
            let (r, thread_serial) = parse_u32(r)?;
            Ok((r, GcRecord::RootNativeStack { object_id, thread_serial }))
        }
        TAG_GC_ROOT_STICKY_CLASS => {
            let (r, object_id) = parse_id(id_size, r)?;
            Ok((r, GcRecord::RootStickyClass { object_id }))
        }
        TAG_GC_ROOT_THREAD_BLOCK => {
            let (r, object_id)    = parse_id(id_size, r)?;
            let (r, thread_serial) = parse_u32(r)?;
            Ok((r, GcRecord::RootThreadBlock { object_id, thread_serial }))
        }
        TAG_GC_ROOT_MONITOR_USED => {
            let (r, object_id) = parse_id(id_size, r)?;
            Ok((r, GcRecord::RootMonitorUsed { object_id }))
        }
        TAG_GC_ROOT_THREAD_OBJ => {
            let (r, object_id)    = parse_id(id_size, r)?;
            let (r, thread_serial) = parse_u32(r)?;
            let (r, stack_serial)  = parse_u32(r)?;
            Ok((r, GcRecord::RootThreadObject { object_id, thread_serial, stack_serial }))
        }
        TAG_GC_CLASS_DUMP      => parse_class_dump(id_size, r),
        TAG_GC_INSTANCE_DUMP   => parse_instance_dump(id_size, include_data, r),
        TAG_GC_OBJ_ARRAY_DUMP  => parse_object_array_dump(id_size, include_data, r),
        TAG_GC_PRIM_ARRAY_DUMP => parse_primitive_array_dump(id_size, r),
        x => panic!("unknown GC sub-record tag: 0x{x:02X}"),
    }
}

fn parse_class_dump(id_size: u32, i: &[u8]) -> IResult<&[u8], GcRecord> {
    let (r, class_object_id)       = parse_id(id_size, i)?;
    let (r, _stack_serial)         = parse_u32(r)?;
    let (r, super_class_object_id) = parse_id(id_size, r)?;
    // Skip: class_loader_id, signers_id, protection_domain_id, reserved_1, reserved_2
    let (r, _)                     = bytes::streaming::take(id_size as u64 * 5)(r)?;
    let (r, instance_size)         = parse_u32(r)?;

    // Constant pool — parse through but discard; not needed for analysis.
    let (r, cp_count) = parse_u16(r)?;
    let mut r = r;
    for _ in 0..cp_count {
        let (r2, _idx)  = parse_u16(r)?;
        let (r2, ty)    = parse_field_type(r2)?;
        let (r2, _val)  = parse_field_value(id_size, ty, r2)?;
        r = r2;
    }

    // Static fields — stored for future query-layer use.
    let (r, static_count) = parse_u16(r)?;
    let mut static_fields = Vec::with_capacity(static_count as usize);
    let mut r = r;
    for _ in 0..static_count {
        let (r2, name_id) = parse_id(id_size, r)?;
        let (r2, ty)      = parse_field_type(r2)?;
        let (r2, val)     = parse_field_value(id_size, ty, r2)?;
        static_fields.push((FieldInfo { name_id, field_type: ty }, val));
        r = r2;
    }

    // Instance field descriptors — critical for pass 2 reference extraction.
    let (r, instance_count) = parse_u16(r)?;
    let mut instance_fields = Vec::with_capacity(instance_count as usize);
    let mut r = r;
    for _ in 0..instance_count {
        let (r2, name_id) = parse_id(id_size, r)?;
        let (r2, ty)      = parse_field_type(r2)?;
        instance_fields.push(FieldInfo { name_id, field_type: ty });
        r = r2;
    }

    Ok((r, GcRecord::ClassDump(Box::new(ClassDumpFields {
        class_object_id,
        super_class_object_id,
        instance_size,
        instance_fields,
        static_fields,
    }))))
}

fn parse_instance_dump(id_size: u32, include_data: bool, i: &[u8]) -> IResult<&[u8], GcRecord> {
    let (r, object_id)     = parse_id(id_size, i)?;
    let (r, _stack_serial) = parse_u32(r)?;
    let (r, class_id)      = parse_id(id_size, r)?;
    let (r, data_size)     = parse_u32(r)?;
    let (r, raw_data) = if include_data {
        let (r, slice) = bytes::streaming::take(data_size as u64)(r)?;
        (r, slice.to_vec())
    } else {
        let (r, _) = bytes::streaming::take(data_size as u64)(r)?;
        (r, Vec::new())
    };
    Ok((r, GcRecord::InstanceDump { object_id, class_id, data_size, raw_data }))
}

fn parse_object_array_dump(id_size: u32, include_data: bool, i: &[u8]) -> IResult<&[u8], GcRecord> {
    let (r, object_id)        = parse_id(id_size, i)?;
    let (r, _stack_serial)    = parse_u32(r)?;
    let (r, num_elements)     = parse_u32(r)?;
    let (r, element_class_id) = parse_id(id_size, r)?;
    let total_bytes           = num_elements as u64 * id_size as u64;
    let (r, elements) = if include_data {
        let (r, slice) = bytes::streaming::take(total_bytes)(r)?;
        let ids = slice
            .chunks_exact(id_size as usize)
            .map(|b| if id_size == 4 {
                u32::from_be_bytes(b.try_into().unwrap()) as u64
            } else {
                u64::from_be_bytes(b.try_into().unwrap())
            })
            .collect();
        (r, ids)
    } else {
        let (r, _) = bytes::streaming::take(total_bytes)(r)?;
        (r, Vec::new())
    };
    Ok((r, GcRecord::ObjectArrayDump { object_id, num_elements, element_class_id, elements }))
}

fn parse_primitive_array_dump(id_size: u32, i: &[u8]) -> IResult<&[u8], GcRecord> {
    let (r, object_id)    = parse_id(id_size, i)?;
    let (r, _stack_serial) = parse_u32(r)?;
    let (r, num_elements) = parse_u32(r)?;
    let (r, element_type) = parse_field_type(r)?;
    let (r, _)            = skip_primitive_array(element_type, num_elements, r)?;
    Ok((r, GcRecord::PrimitiveArrayDump { object_id, num_elements, element_type }))
}

// ---------------------------------------------------------------------------
// Top-level record parsers
// ---------------------------------------------------------------------------

fn parse_utf8_string(id_size: u32, i: &[u8]) -> IResult<&[u8], Record> {
    let (r, (_time, length)) = parse_record_header(i)?;
    let (r, id)              = parse_id(id_size, r)?;
    let str_bytes            = (length as u64).saturating_sub(id_size as u64);
    let (r, raw)             = bytes::streaming::take(str_bytes)(r)?;
    let str                  = String::from_utf8_lossy(raw).into();
    Ok((r, Record::Utf8String { id, str }))
}

fn parse_load_class(id_size: u32, i: &[u8]) -> IResult<&[u8], Record> {
    let (r, _header)         = parse_record_header(i)?;
    let (r, serial_number)   = parse_u32(r)?;
    let (r, class_object_id) = parse_id(id_size, r)?;
    let (r, _stack_serial)   = parse_u32(r)?;
    let (r, class_name_id)   = parse_id(id_size, r)?;
    Ok((r, Record::LoadClass(LoadClassData {
        serial_number,
        class_object_id,
        class_name_id,
    })))
}

// ---------------------------------------------------------------------------
// Streaming combinator
// ---------------------------------------------------------------------------

/// A copy of nom's `many1` adapted for streaming use:
/// - accumulates into a caller-supplied `&mut Vec` to enable buffer pooling
/// - returns `Ok` with whatever has been parsed on `Incomplete` rather than failing,
///   allowing the caller to refill the buffer and continue
pub fn lazy_many1<'a, I, O, E, F>(
    mut f: F,
    pooled_vec: &'a mut Vec<O>,
) -> impl FnMut(I) -> IResult<I, (), E> + 'a
where
    I: Clone + PartialEq,
    F: Parser<I, Output = O, Error = E> + 'a,
    E: ParseError<I>,
{
    move |mut i: I| match f.parse(i.clone()) {
        Err(nom::Err::Error(err)) => Err(nom::Err::Error(E::append(i, ErrorKind::Many1, err))),
        Err(e) => Err(e),
        Ok((i1, o)) => {
            pooled_vec.push(o);
            i = i1;
            loop {
                match f.parse(i.clone()) {
                    Err(nom::Err::Error(_))      => return Ok((i, ())),
                    // Return what we have so far — caller will refill and retry.
                    Err(nom::Err::Incomplete(_)) => return Ok((i, ())),
                    Err(e)                       => return Err(e),
                    Ok((i1, o)) => {
                        if i1 == i {
                            return Err(nom::Err::Error(E::from_error_kind(i, ErrorKind::Many1)));
                        }
                        i = i1;
                        pooled_vec.push(o);
                    }
                }
            }
        }
    }
}
