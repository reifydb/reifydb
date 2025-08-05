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

static LAYOUT: Lazy<EncodedRowLayout> = Lazy::new(|| EncodedRowLayout::new(&[Type::Uint1]));

pub(crate) struct GeneratorU8 {}

impl GeneratorU8 {
    pub(crate) fn next<VT, UT>(
        atx: &mut ActiveWriteTransaction<VT, UT>,
        key: &EncodedKey,
    ) -> crate::Result<u8>
    where
        VT: VersionedTransaction,
        UT: UnversionedTransaction,
    {
        atx.with_unversioned_write(|tx| match tx.get(key)? {
            Some(unversioned_row) => {
                let mut row = unversioned_row.row;
                let current_value = LAYOUT.get_u8(&row, 0);
                let next_value = current_value.saturating_add(1);

                if current_value == next_value {
                    return_error!(sequence_exhausted(Type::Uint1));
                }

                LAYOUT.set_u8(&mut row, 0, next_value);
                tx.set(key, row)?;
                Ok(next_value)
            }
            None => {
                let mut new_row = LAYOUT.allocate_row();
                LAYOUT.set_u8(&mut new_row, 0, 1u8);
                tx.set(key, new_row)?;
                Ok(1)
            }
        })
    }

    pub(crate) fn set<VT, UT>(
        atx: &mut ActiveWriteTransaction<VT, UT>,
        key: &EncodedKey,
        value: u8,
    ) -> crate::Result<()>
    where
        VT: VersionedTransaction,
        UT: UnversionedTransaction,
    {
        atx.with_unversioned_write(|tx| {
            let mut row = match tx.get(key)? {
                Some(unversioned_row) => unversioned_row.row,
                None => LAYOUT.allocate_row(),
            };
            LAYOUT.set_u8(&mut row, 0, value);
            tx.set(key, row)?;
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::sequence::generator::u8::{GeneratorU8, LAYOUT};
    use reifydb_core::interface::{
        Unversioned, UnversionedReadTransaction, UnversionedWriteTransaction,
    };
    use reifydb_core::result::error::diagnostic::sequence::sequence_exhausted;
    use reifydb_core::{EncodedKey, Type};
    use reifydb_transaction::test_utils::create_test_write_transaction;

    #[test]
    fn test_ok() {
        let mut atx = create_test_write_transaction();
        for expected in 1..100 {
            let got = GeneratorU8::next(&mut atx, &EncodedKey::new("sequence")).unwrap();
            assert_eq!(got, expected);
        }

        atx.with_unversioned_read(|tx| {
            let mut unversioned: Vec<Unversioned> = tx.scan()?.collect();
            assert_eq!(unversioned.len(), 2);

            unversioned.pop().unwrap();
            let unversioned = unversioned.pop().unwrap();
            assert_eq!(unversioned.key, EncodedKey::new("sequence"));
            assert_eq!(LAYOUT.get_u8(&unversioned.row, 0), 99);

            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn test_exhaustion() {
        let mut atx = create_test_write_transaction();

        let mut row = LAYOUT.allocate_row();
        LAYOUT.set_u8(&mut row, 0, u8::MAX);

        atx.with_unversioned_write(|tx| tx.set(&EncodedKey::new("sequence"), row)).unwrap();

        let err = GeneratorU8::next(&mut atx, &EncodedKey::new("sequence")).unwrap_err();
        assert_eq!(err.diagnostic(), sequence_exhausted(Type::Uint1));
    }
}