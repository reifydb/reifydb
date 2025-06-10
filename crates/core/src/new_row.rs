// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::{AsyncCowVec, Value, ValueKind};
use std::ptr;

fn size_and_align(value: ValueKind) -> (usize, usize) {
    match value {
        // ValueKind::Bool => (1, 1),
        // ValueKind::Float4 => (4, 4),
        // ValueKind::Float8 => (8, 8),
        ValueKind::Int1 | ValueKind::Uint1 => (1, 1),
        ValueKind::Int2 | ValueKind::Uint2 => (2, 2),
        ValueKind::Int4 | ValueKind::Uint4 => (4, 4),
        ValueKind::Int8 | ValueKind::Uint8 => (8, 8),
        ValueKind::Int16 | ValueKind::Uint16 => (16, 16),
        // ValueKind::String => (255, 1),
        // ValueKind::Undefined => (0, 1),
        _ => unimplemented!(),
    }
}

pub type NewRow = AsyncCowVec<u8>;

/// A boxed row iterator.
pub type NewRowIter = Box<dyn NewRowIterator>;

pub trait NewRowIterator: Iterator<Item = NewRow> {}

impl<I: Iterator<Item = NewRow>> NewRowIterator for I {}

pub struct Field {
    pub offset: usize,
    pub size: usize,
    pub align: usize,
    pub value: ValueKind,
}

pub struct Layout {
    pub fields: Vec<Field>,
    pub total_size: usize,
    pub alignment: usize,
}

impl Layout {
    pub fn new(kinds: &[ValueKind]) -> Self {
        let mut offset = 0;
        let mut fields = vec![];
        let mut max_align = 1;

        for &value in kinds {
            let (size, align) = size_and_align(value);
            offset = align_up(offset, align);
            fields.push(Field { offset, size, align, value });
            offset += size;
            max_align = max_align.max(align);
        }

        let total_size = align_up(offset, max_align);
        Layout { fields, total_size, alignment: max_align }
    }

    pub fn allocate_row(&self) -> AsyncCowVec<u8> {
        let layout = std::alloc::Layout::from_size_align(self.total_size, self.alignment).unwrap();
        unsafe {
            let ptr = std::alloc::alloc_zeroed(layout);
            if ptr.is_null() {
                std::alloc::handle_alloc_error(layout);
            }
            AsyncCowVec::new(Vec::from_raw_parts(ptr, self.total_size, self.total_size))
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

    pub fn write_value(&self, row: &mut [u8], index: usize, val: &Value) {
        let field = &self.fields[index];
        unsafe {
            let dst = row.as_mut_ptr().add(field.offset);
            match (field.value, val) {
                (ValueKind::Int1, Value::Int1(v)) => ptr::write_unaligned(dst as *mut i8, *v),
                (ValueKind::Int2, Value::Int2(v)) => ptr::write_unaligned(dst as *mut i16, *v),
                (ValueKind::Int4, Value::Int4(v)) => ptr::write_unaligned(dst as *mut i32, *v),
                (ValueKind::Int8, Value::Int8(v)) => ptr::write_unaligned(dst as *mut i64, *v),

                (ValueKind::Uint1, Value::Uint1(v)) => ptr::write_unaligned(dst, *v),
                (ValueKind::Uint2, Value::Uint2(v)) => ptr::write_unaligned(dst as *mut u16, *v),
                (ValueKind::Uint4, Value::Uint4(v)) => ptr::write_unaligned(dst as *mut u32, *v),
                (ValueKind::Uint8, Value::Uint8(v)) => ptr::write_unaligned(dst as *mut u64, *v),
                // (ValueKind::Float8, Value::Float8(v)) => ptr::write_unaligned(dst as *mut f64, v.value()),
                // ... handle others ...
                _ => panic!("mismatched type"),
            }
        }
    }

    pub fn read_value(&self, row: &[u8], index: usize) -> Value {
        let field = &self.fields[index];
        unsafe {
            let src = row.as_ptr().add(field.offset);
            match field.value {
                ValueKind::Int1 => Value::Int1(ptr::read_unaligned(src as *const i8)),
                ValueKind::Int2 => Value::Int2(ptr::read_unaligned(src as *const i16)),
                ValueKind::Int4 => Value::Int4(ptr::read_unaligned(src as *const i32)),
                ValueKind::Int8 => Value::Int8(ptr::read_unaligned(src as *const i64)),
                // ValueKind::Float8 => Value::Float8(ptr::read_unaligned(src as *const f64)),
                // ...
                ValueKind::Uint1 => Value::Uint1(ptr::read_unaligned(src)),
                ValueKind::Uint2 => Value::Uint2(ptr::read_unaligned(src as *const u16)),
                ValueKind::Uint4 => Value::Uint4(ptr::read_unaligned(src as *const u32)),
                ValueKind::Uint8 => Value::Uint8(ptr::read_unaligned(src as *const u64)),

                _ => unimplemented!(),
            }
        }
    }

    fn get_i8(&self, row: &[u8], index: usize) -> i8 {
        debug_assert!(row.len() == self.total_size);
        let field = &self.fields[index];
        unsafe { (row.as_ptr().add(field.offset) as *const i8).read_unaligned() }
    }

    fn get_mut_i8(&self, row: &mut [u8], index: usize) -> &mut i8 {
        debug_assert!(row.len() == self.total_size);
        let field = &self.fields[index];
        unsafe { &mut *(row.as_mut_ptr().add(field.offset) as *mut i8) }
        // unsafe { let src = row.as_ptr().add(field.offset);
    }

    fn get_i32(&self, row: &[u8], index: usize) -> i32 {
        debug_assert!(row.len() == self.total_size);
        let field = &self.fields[index];
        unsafe { (row.as_ptr().add(field.offset) as *const i32).read_unaligned() }
    }

    fn get_mut_i32(&self, row: &mut [u8], index: usize) -> &mut i32 {
        debug_assert!(row.len() == self.total_size);
        let field = &self.fields[index];
        unsafe { &mut *(row.as_mut_ptr().add(field.offset) as *mut i32) }
        // unsafe { let src = row.as_ptr().add(field.offset);
    }
}

#[cfg(test)]
mod tests {
    use crate::new_row::Layout;
    use crate::{Value, ValueKind};

    #[test]
    fn test() {
        // let layout = Layout::new(&[ValueKind::Int4, ValueKind::Float8, ValueKind::Bool]);
        let layout = Layout::new(&[ValueKind::Int1, ValueKind::Int2, ValueKind::Int4]);

        let mut row = layout.allocate_row();

        let mut_row = row.make_mut();
        // layout.write_value(&mut buffer, 2, &Value::Int4(42));
        let v = layout.get_mut_i32(mut_row, 0);
        *v = 127;

        let v = layout.get_mut_i32(mut_row, 2);
        *v = 42;

        // layout.write_value(&mut buffer, 1, &Value::Float8(3.14));
        // layout.write_value(&mut buffer, 2, &Value::Bool(true));

        assert_eq!(layout.read_value(&row, 0), Value::Int1(127));
        assert_eq!(layout.read_value(&row, 2), Value::Int4(42));
        assert_eq!(layout.get_i32(&row, 2), 42i32);
        // assert_eq!(layout.read_value(&buffer, 1), Value::Float8(3.14));
        // assert_eq!(layout.read_value(&buffer, 2), Value::Bool(true));
    }
}
