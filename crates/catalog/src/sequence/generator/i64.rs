// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use once_cell::sync::Lazy;
use reifydb_core::diagnostic::sequence::sequence_exhausted;
use reifydb_core::interface::{
    ActiveWriteTransaction, UnversionedReadTransaction, UnversionedTransaction,
    UnversionedWriteTransaction, VersionedTransaction,
};
use reifydb_core::row::EncodedRowLayout;
use reifydb_core::{EncodedKey, Type, return_error};

static LAYOUT: Lazy<EncodedRowLayout> = Lazy::new(|| EncodedRowLayout::new(&[Type::Int8]));

pub(crate) struct GeneratorI64 {}

impl GeneratorI64 {
    pub(crate) fn next<VT, UT>(
        atx: &mut ActiveWriteTransaction<VT, UT>,
        key: &EncodedKey,
    ) -> crate::Result<i64>
    where
        VT: VersionedTransaction,
        UT: UnversionedTransaction,
    {
        atx.with_unversioned_write(|tx| match tx.get(key)? {
            Some(unversioned_row) => {
                let mut row = unversioned_row.row;
                let value = LAYOUT.get_i64(&row, 0);
                let next_value = value.saturating_add(1);

                if value == next_value {
                    return_error!(sequence_exhausted(Type::Int8));
                }

                LAYOUT.set_i64(&mut row, 0, next_value);
                tx.set(key, row)?;
                Ok(value)
            }
            None => {
                let mut new_row = LAYOUT.allocate_row();
                LAYOUT.set_i64(&mut new_row, 0, 2i64);
                tx.set(key, new_row)?;
                Ok(1)
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::sequence::generator::i64::{GeneratorI64, LAYOUT};
    use reifydb_core::interface::{
        Unversioned, UnversionedReadTransaction, UnversionedWriteTransaction,
    };
    use reifydb_core::result::error::diagnostic::sequence::sequence_exhausted;
    use reifydb_core::{EncodedKey, Type};
    use reifydb_transaction::test_utils::create_test_write_transaction;

    #[test]
    fn test_ok() {
        let mut atx = create_test_write_transaction();
        for expected in 1..1000 {
            let got = GeneratorI64::next(&mut atx, &EncodedKey::new("sequence")).unwrap();
            assert_eq!(got, expected);
        }

        atx.with_unversioned_read(|tx| {
            let mut unversioned: Vec<Unversioned> = tx.scan()?.collect();
            assert_eq!(unversioned.len(), 2);

            unversioned.pop().unwrap();
            let unversioned = unversioned.pop().unwrap();
            assert_eq!(unversioned.key, EncodedKey::new("sequence"));
            assert_eq!(LAYOUT.get_i64(&unversioned.row, 0), 1000);

            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn test_exhaustion() {
        let mut atx = create_test_write_transaction();

        let mut row = LAYOUT.allocate_row();
        LAYOUT.set_i64(&mut row, 0, i64::MAX);

        atx.with_unversioned_write(|tx| tx.set(&EncodedKey::new("sequence"), row)).unwrap();

        let err = GeneratorI64::next(&mut atx, &EncodedKey::new("sequence")).unwrap_err();
        assert_eq!(err.diagnostic(), sequence_exhausted(Type::Int8));
    }
}