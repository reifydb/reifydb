// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

pub use layout::{Field, Layout};
pub use row::{Row, RowIter, RowIterator, deprecated_serialize_row, deprecated_deserialize_row};

mod get;
mod layout;
mod row;
mod set;

#[cfg(test)]
mod tests {
    use crate::ValueKind;
    use crate::row::layout::Layout;

    #[test]
    fn test() {
        // let layout = Layout::new(&[ValueKind::Int4, ValueKind::Float8, ValueKind::Bool]);
        let layout = Layout::new(&[ValueKind::Int1, ValueKind::Int2, ValueKind::Int4]);

        let mut row = layout.allocate_row();

        let mut_row = row.make_mut();
        // layout.write_value(&mut buffer, 2, &Value::Int4(42));
        // let v = layout.get_mut_i32(mut_row, 0);
        // *v = 127;

        // let v = layout.get_mut_i32(mut_row, 2);
        // *v = 42;
        layout.set_i32(mut_row, 2, 42);

        // layout.write_value(&mut buffer, 1, &Value::Float8(3.14));
        // layout.write_value(&mut buffer, 2, &Value::Bool(true));

        // assert_eq!(layout.read_value(&row, 0), Value::Int1(0));
        // assert_eq!(layout.read_value(&row, 2), Value::Int4(42));
        assert_eq!(layout.get_i32(&row, 2), 42i32);
        // assert_eq!(layout.read_value(&buffer, 1), Value::Float8(3.14));
        // assert_eq!(layout.read_value(&buffer, 2), Value::Bool(true));
    }
}
