// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::frame::ColumnValues;
use crate::CowVec;

impl ColumnValues {
    pub fn reorder(&mut self, indices: &[usize]) {
        match self {
            ColumnValues::Bool(v, bitvec) => {
                v.reorder(indices);
                bitvec.reorder(indices);
            }
            ColumnValues::Float4(v, bitvec) => {
                v.reorder(indices);
                bitvec.reorder(indices);
            }
            ColumnValues::Float8(v, valid) => {
                v.reorder(indices);
                valid.reorder(indices);
            }
            ColumnValues::Int1(v, valid) => {
                v.reorder(indices);
                valid.reorder(indices);
            }
            ColumnValues::Int2(v, valid) => {
                v.reorder(indices);
                valid.reorder(indices);
            }
            ColumnValues::Int4(v, valid) => {
                v.reorder(indices);
                valid.reorder(indices);
            }
            ColumnValues::Int8(v, valid) => {
                v.reorder(indices);
                valid.reorder(indices);
            }
            ColumnValues::Int16(v, valid) => {
                v.reorder(indices);
                valid.reorder(indices);
            }
            ColumnValues::Utf8(v, valid) => {
                v.reorder(indices);
                valid.reorder(indices);
            }
            ColumnValues::Uint1(v, valid) => {
                v.reorder(indices);
                valid.reorder(indices);
            }
            ColumnValues::Uint2(v, valid) => {
                v.reorder(indices);
                valid.reorder(indices);
            }
            ColumnValues::Uint4(v, valid) => {
                v.reorder(indices);
                valid.reorder(indices);
            }
            ColumnValues::Uint8(v, valid) => {
                v.reorder(indices);
                valid.reorder(indices);
            }
            ColumnValues::Uint16(v, valid) => {
                v.reorder(indices);
                valid.reorder(indices);
            }
            ColumnValues::Date(v, valid) => {
                v.reorder(indices);
                valid.reorder(indices);
            }
            ColumnValues::DateTime(v, valid) => {
                v.reorder(indices);
                valid.reorder(indices);
            }
            ColumnValues::Time(v, valid) => {
                v.reorder(indices);
                valid.reorder(indices);
            }
            ColumnValues::Interval(v, valid) => {
                v.reorder(indices);
                valid.reorder(indices);
            }
            ColumnValues::Undefined(len) => {
                *len = indices.len();
            }
            ColumnValues::RowId(values, bitvec) => {
                let old_values = values.clone();
                let new_values: Vec<_> = indices.iter().map(|&i| old_values[i]).collect();
                *values = CowVec::new(new_values);
                bitvec.reorder(indices);
            }
            ColumnValues::Uuid4(values, bitvec) => {
                values.reorder(indices);
                bitvec.reorder(indices);
            }
            ColumnValues::Uuid7(values, bitvec) => {
                values.reorder(indices);
                bitvec.reorder(indices);
            }
            ColumnValues::Blob(values, bitvec) => {
                values.reorder(indices);
                bitvec.reorder(indices);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::frame::column::ColumnValues;
    use crate::CowVec;

    #[test]
    fn test_reorder_bool() {
        let mut col =
            ColumnValues::Bool([true, false, true].into(), [true, false, true].into());
        col.reorder(&[2, 0, 1]);
        assert_eq!(
            col,
            ColumnValues::Bool([true, true, false].into(), [true, true, false].into())
        );
    }

    #[test]
    fn test_reorder_float4() {
        let mut col =
            ColumnValues::Float4(CowVec::new(vec![1.0, 2.0, 3.0]), [true, false, true].into());
        col.reorder(&[2, 0, 1]);
        assert_eq!(
            col,
            ColumnValues::Float4(CowVec::new(vec![3.0, 1.0, 2.0]), [true, true, false].into())
        );
    }

    #[test]
    fn test_reorder_float8() {
        let mut col =
            ColumnValues::Float8(CowVec::new(vec![1.0, 2.0, 3.0]), [true, false, true].into());
        col.reorder(&[2, 0, 1]);
        assert_eq!(
            col,
            ColumnValues::Float8(CowVec::new(vec![3.0, 1.0, 2.0]), [true, true, false].into())
        );
    }

    #[test]
    fn test_reorder_int1() {
        let mut col = ColumnValues::Int1(CowVec::new(vec![1, 2, 3]), [true, false, true].into());
        col.reorder(&[2, 0, 1]);
        assert_eq!(col, ColumnValues::Int1(CowVec::new(vec![3, 1, 2]), [true, true, false].into()));
    }

    #[test]
    fn test_reorder_int2() {
        let mut col = ColumnValues::Int2(CowVec::new(vec![1, 2, 3]), [true, false, true].into());
        col.reorder(&[2, 0, 1]);
        assert_eq!(col, ColumnValues::Int2(CowVec::new(vec![3, 1, 2]), [true, true, false].into()));
    }

    #[test]
    fn test_reorder_int4() {
        let mut col = ColumnValues::Int4(CowVec::new(vec![1, 2, 3]), [true, false, true].into());
        col.reorder(&[2, 0, 1]);
        assert_eq!(col, ColumnValues::Int4(CowVec::new(vec![3, 1, 2]), [true, true, false].into()));
    }

    #[test]
    fn test_reorder_int8() {
        let mut col = ColumnValues::Int8(CowVec::new(vec![1, 2, 3]), [true, false, true].into());
        col.reorder(&[2, 0, 1]);
        assert_eq!(col, ColumnValues::Int8(CowVec::new(vec![3, 1, 2]), [true, true, false].into()));
    }

    #[test]
    fn test_reorder_int16() {
        let mut col = ColumnValues::Int16(CowVec::new(vec![1, 2, 3]), [true, false, true].into());
        col.reorder(&[2, 0, 1]);
        assert_eq!(
            col,
            ColumnValues::Int16(CowVec::new(vec![3, 1, 2]), [true, true, false].into())
        );
    }

    #[test]
    fn test_reorder_string() {
        let mut col = ColumnValues::Utf8(
            CowVec::new(vec!["a".into(), "b".into(), "c".into()]),
            [true, false, true].into(),
        );
        col.reorder(&[2, 0, 1]);
        assert_eq!(
            col,
            ColumnValues::Utf8(
                CowVec::new(vec!["c".into(), "a".into(), "b".into()]),
                [true, true, false].into()
            )
        );
    }

    #[test]
    fn test_reorder_uint1() {
        let mut col = ColumnValues::Uint1(CowVec::new(vec![1, 2, 3]), [true, false, true].into());
        col.reorder(&[2, 0, 1]);
        assert_eq!(
            col,
            ColumnValues::Uint1(CowVec::new(vec![3, 1, 2]), [true, true, false].into())
        );
    }

    #[test]
    fn test_reorder_uint2() {
        let mut col = ColumnValues::Uint2(CowVec::new(vec![1, 2, 3]), [true, false, true].into());
        col.reorder(&[2, 0, 1]);
        assert_eq!(
            col,
            ColumnValues::Uint2(CowVec::new(vec![3, 1, 2]), [true, true, false].into())
        );
    }

    #[test]
    fn test_reorder_uint4() {
        let mut col = ColumnValues::Uint4(CowVec::new(vec![1, 2, 3]), [true, false, true].into());
        col.reorder(&[2, 0, 1]);
        assert_eq!(
            col,
            ColumnValues::Uint4(CowVec::new(vec![3, 1, 2]), [true, true, false].into())
        );
    }

    #[test]
    fn test_reorder_uint8() {
        let mut col = ColumnValues::Uint8(CowVec::new(vec![1, 2, 3]), [true, false, true].into());
        col.reorder(&[2, 0, 1]);
        assert_eq!(
            col,
            ColumnValues::Uint8(CowVec::new(vec![3, 1, 2]), [true, true, false].into())
        );
    }

    #[test]
    fn test_reorder_uint16() {
        let mut col = ColumnValues::Uint16(CowVec::new(vec![1, 2, 3]), [true, false, true].into());
        col.reorder(&[2, 0, 1]);
        assert_eq!(
            col,
            ColumnValues::Uint16(CowVec::new(vec![3, 1, 2]), [true, true, false].into())
        );
    }

    #[test]
    fn test_reorder_undefined() {
        let mut col = ColumnValues::Undefined(3);
        col.reorder(&[2, 0, 1]);
        assert_eq!(col, ColumnValues::Undefined(3));
        col.reorder(&[1, 0]);
        assert_eq!(col, ColumnValues::Undefined(2));
    }
}
