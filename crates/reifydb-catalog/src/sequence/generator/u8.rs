// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use once_cell::sync::Lazy;
use reifydb_core::diagnostic::sequence::sequence_exhausted;
use reifydb_core::interface::{Transaction, 
    ActiveCommandTransaction, UnversionedQueryTransaction,
    UnversionedCommandTransaction,
};
use reifydb_core::row::EncodedRowLayout;
use reifydb_core::{EncodedKey, Type, return_error};

static LAYOUT: Lazy<EncodedRowLayout> = Lazy::new(|| EncodedRowLayout::new(&[Type::Uint1]));

pub(crate) struct GeneratorU8 {}

impl GeneratorU8 {
    pub(crate) fn next<T: Transaction>(
        txn: &mut ActiveCommandTransaction<T>,
        key: &EncodedKey,
    ) -> crate::Result<u8>
    where
    {
        txn.with_unversioned_command(|tx| match tx.get(key)? {
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

    pub(crate) fn set<T: Transaction>(
        txn: &mut ActiveCommandTransaction<T>,
        key: &EncodedKey,
        value: u8,
    ) -> crate::Result<()>
    where
    {
        txn.with_unversioned_command(|tx| {
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
    use reifydb_core::interface::{Transaction, 
        Unversioned, UnversionedQueryTransaction, UnversionedCommandTransaction,
    };
    use reifydb_core::result::error::diagnostic::sequence::sequence_exhausted;
    use reifydb_core::{EncodedKey, Type};
    use reifydb_transaction::test_utils::create_test_command_transaction;

    #[test]
    fn test_ok() {
        let mut txn = create_test_command_transaction();
        for expected in 1..100 {
            let got = GeneratorU8::next(&mut txn, &EncodedKey::new("sequence")).unwrap();
            assert_eq!(got, expected);
        }

        txn.with_unversioned_query(|tx| {
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
        let mut txn = create_test_command_transaction();

        let mut row = LAYOUT.allocate_row();
        LAYOUT.set_u8(&mut row, 0, u8::MAX);

        txn.with_unversioned_command(|tx| tx.set(&EncodedKey::new("sequence"), row)).unwrap();

        let err = GeneratorU8::next(&mut txn, &EncodedKey::new("sequence")).unwrap_err();
        assert_eq!(err.diagnostic(), sequence_exhausted(Type::Uint1));
    }
}