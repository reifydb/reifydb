// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::ordered_float::{OrderedF32, OrderedF64};
use crate::row::Row;
use crate::{AsyncCowVec, Value, ValueKind};

#[derive(Debug)]
pub struct Field {
    pub offset: usize,
    pub size: usize,
    pub align: usize,
    pub value: ValueKind,
}

#[derive(Debug)]
pub struct Layout {
    pub fields: Vec<Field>,
    pub size: usize,
    pub alignment: usize,
}

impl Layout {
    pub fn new(kinds: &[ValueKind]) -> Self {
        let mut offset = 0;
        let mut fields = vec![];
        let mut max_align = 1;

        for &value in kinds {
            let size = value.size();
            let align = value.alignment();
            offset = align_up(offset, align);
            fields.push(Field { offset, size, align, value });
            offset += size;
            max_align = max_align.max(align);
        }

        let size = align_up(offset, max_align);
        Layout { fields, size, alignment: max_align }
    }

    pub fn allocate_row(&self) -> Row {
        let layout = std::alloc::Layout::from_size_align(self.size, self.alignment).unwrap();
        unsafe {
            let ptr = std::alloc::alloc_zeroed(layout);
            if ptr.is_null() {
                std::alloc::handle_alloc_error(layout);
            }
            Row(AsyncCowVec::new(Vec::from_raw_parts(ptr, self.size, self.size)))
        }
    }
}

fn align_up(offset: usize, align: usize) -> usize {
    (offset + align - 1) & !(align - 1)
}

impl Layout {
    pub fn get_field(&self, index: usize) -> &Field {
        &self.fields[index]
    }

    pub fn write_values(&self, row: &mut [u8], values: &[Value]) {
        debug_assert!(values.len() == self.fields.len());
        for (idx, value) in values.iter().enumerate() {
            self.set_value(row, idx, value)
        }
    }

    pub fn set_value(&self, row: &mut [u8], index: usize, val: &Value) {
        let field = &self.fields[index];
        debug_assert_eq!(row.len(), self.size);

        match (field.value, val) {
            (ValueKind::Bool, Value::Bool(v)) => self.set_bool(row, index, *v),
            (ValueKind::Float4, Value::Float4(v)) => self.set_f32(row, index, v.value()),
            (ValueKind::Float8, Value::Float8(v)) => self.set_f64(row, index, v.value()),

            (ValueKind::Int1, Value::Int1(v)) => self.set_i8(row, index, *v),
            (ValueKind::Int2, Value::Int2(v)) => self.set_i16(row, index, *v),
            (ValueKind::Int4, Value::Int4(v)) => self.set_i32(row, index, *v),
            (ValueKind::Int8, Value::Int8(v)) => self.set_i64(row, index, *v),
            (ValueKind::Int16, Value::Int16(v)) => self.set_i128(row, index, *v),

            (ValueKind::Uint1, Value::Uint1(v)) => self.set_u8(row, index, *v),
            (ValueKind::Uint2, Value::Uint2(v)) => self.set_u16(row, index, *v),
            (ValueKind::Uint4, Value::Uint4(v)) => self.set_u32(row, index, *v),
            (ValueKind::Uint8, Value::Uint8(v)) => self.set_u64(row, index, *v),
            (ValueKind::Uint16, Value::Uint16(v)) => self.set_u128(row, index, *v),

            (ValueKind::String, Value::String(v)) => self.set_str(row, index, v),

            (ValueKind::Undefined, Value::Undefined) => {}
            (_, _) => unreachable!(),
        }
    }

    pub fn get_value(&self, row: &[u8], index: usize) -> Value {
        let field = &self.fields[index];
        unsafe {
            let src = row.as_ptr().add(field.offset);
            match field.value {
                ValueKind::Bool => Value::Bool(self.get_bool(row, index)),
                ValueKind::Float4 => OrderedF32::try_from(self.get_f32(row, index))
                    .map(Value::Float4)
                    .unwrap_or(Value::Undefined),
                ValueKind::Float8 => OrderedF64::try_from(self.get_f64(row, index))
                    .map(Value::Float8)
                    .unwrap_or(Value::Undefined),
                ValueKind::Int1 => Value::Int1(self.get_i8(row, index)),
                ValueKind::Int2 => Value::Int2(self.get_i16(row, index)),
                ValueKind::Int4 => Value::Int4(self.get_i32(row, index)),
                ValueKind::Int8 => Value::Int8(self.get_i64(row, index)),
                ValueKind::Int16 => Value::Int16(self.get_i128(row, index)),
                ValueKind::String => Value::String(self.get_str(row, index).to_string()),
                ValueKind::Uint1 => Value::Uint1(self.get_u8(row, index)),
                ValueKind::Uint2 => Value::Uint2(self.get_u16(row, index)),
                ValueKind::Uint4 => Value::Uint4(self.get_u32(row, index)),
                ValueKind::Uint8 => Value::Uint8(self.get_u64(row, index)),
                ValueKind::Uint16 => Value::Uint16(self.get_u128(row, index)),
                ValueKind::Undefined => Value::Undefined,
                _ => unimplemented!(),
            }
        }
    }

    pub fn get_mut_i8(&self, row: &mut [u8], index: usize) -> &mut i8 {
        let field = &self.fields[index];
        debug_assert!(row.len() == self.size);
        debug_assert!(field.value == ValueKind::Int1);
        unsafe { &mut *(row.as_mut_ptr().add(field.offset) as *mut i8) }
        // unsafe { let src = row.as_ptr().add(field.offset);
    }

    pub fn get_mut_i32(&self, row: &mut [u8], index: usize) -> &mut i32 {
        let field = &self.fields[index];
        debug_assert!(row.len() == self.size);
        debug_assert!(field.value == ValueKind::Int4);
        unsafe { &mut *(row.as_mut_ptr().add(field.offset) as *mut i32) }
        // unsafe { let src = row.as_ptr().add(field.offset);
    }
}
