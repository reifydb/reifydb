// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use once_cell::sync::Lazy;
use reifydb_core::diagnostic::sequence::sequence_exhausted;
use reifydb_core::interface::{
    ActiveCommandTransaction, UnversionedQueryTransaction, UnversionedTransaction,
    UnversionedCommandTransaction, VersionedTransaction,
};
use reifydb_core::row::EncodedRowLayout;
use reifydb_core::{EncodedKey, Type, return_error};

static LAYOUT: Lazy<EncodedRowLayout> = Lazy::new(|| EncodedRowLayout::new(&[Type::Int16]));

pub(crate) struct GeneratorI128 {}

impl GeneratorI128 {
    pub(crate) fn next<VT, UT>(
		txn: &mut ActiveCommandTransaction<VT, UT>,
		key: &EncodedKey,
    ) -> crate::Result<i128>
    where
        VT: VersionedTransaction,
        UT: UnversionedTransaction,
    {
        txn.with_unversioned_command(|tx| match tx.get(key)? {
            Some(unversioned_row) => {
                let mut row = unversioned_row.row;
                let current_value = LAYOUT.get_i128(&row, 0);
                let next_value = current_value.saturating_add(1);

                if current_value == next_value {
                    return_error!(sequence_exhausted(Type::Int16));
                }

                LAYOUT.set_i128(&mut row, 0, next_value);
                tx.set(key, row)?;
                Ok(next_value)
            }
            None => {
                let mut new_row = LAYOUT.allocate_row();
                LAYOUT.set_i128(&mut new_row, 0, 1i128);
                tx.set(key, new_row)?;
                Ok(1)
            }
        })
    }

    pub(crate) fn set<VT, UT>(
		txn: &mut ActiveCommandTransaction<VT, UT>,
		key: &EncodedKey,
		value: i128,
    ) -> crate::Result<()>
    where
        VT: VersionedTransaction,
        UT: UnversionedTransaction,
    {
        txn.with_unversioned_command(|tx| {
            let mut row = match tx.get(key)? {
                Some(unversioned_row) => unversioned_row.row,
                None => LAYOUT.allocate_row(),
            };
            LAYOUT.set_i128(&mut row, 0, value);
            tx.set(key, row)?;
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::sequence::generator::i128::{GeneratorI128, LAYOUT};
    use reifydb_core::interface::{
        Unversioned, UnversionedQueryTransaction, UnversionedCommandTransaction,
    };
    use reifydb_core::result::error::diagnostic::sequence::sequence_exhausted;
    use reifydb_core::{EncodedKey, Type};
    use reifydb_transaction::test_utils::create_test_command_transaction;

    #[test]
    fn test_ok() {
        let mut txn = create_test_command_transaction();
        for expected in 1..1000 {
            let got = GeneratorI128::next(&mut txn, &EncodedKey::new("sequence")).unwrap();
            assert_eq!(got, expected);
        }

        txn.with_unversioned_query(|tx| {
            let mut unversioned: Vec<Unversioned> = tx.scan()?.collect();
            assert_eq!(unversioned.len(), 2);

            unversioned.pop().unwrap();
            let unversioned = unversioned.pop().unwrap();
            assert_eq!(unversioned.key, EncodedKey::new("sequence"));
            assert_eq!(LAYOUT.get_i128(&unversioned.row, 0), 999);

            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn test_exhaustion() {
        let mut txn = create_test_command_transaction();

        let mut row = LAYOUT.allocate_row();
        LAYOUT.set_i128(&mut row, 0, i128::MAX);

        txn.with_unversioned_command(|tx| tx.set(&EncodedKey::new("sequence"), row)).unwrap();

        let err = GeneratorI128::next(&mut txn, &EncodedKey::new("sequence")).unwrap_err();
        assert_eq!(err.diagnostic(), sequence_exhausted(Type::Int16));
    }
}