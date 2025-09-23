// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use once_cell::sync::Lazy;
use reifydb_core::{
	EncodedKey,
	diagnostic::sequence::sequence_exhausted,
	interface::{CommandTransaction, SingleVersionCommandTransaction, SingleVersionQueryTransaction},
	return_error,
	value::row::EncodedRowLayout,
};
use reifydb_type::Type;

static LAYOSVT: Lazy<EncodedRowLayout> = Lazy::new(|| EncodedRowLayout::new(&[Type::Int16]));

pub(crate) struct GeneratorI128 {}

impl GeneratorI128 {
	pub(crate) fn next(
		txn: &mut impl CommandTransaction,
		key: &EncodedKey,
		default: Option<i128>,
	) -> crate::Result<i128> {
		txn.with_single_command(|tx| match tx.get(key)? {
			Some(row) => {
				let mut row = row.row;
				let current_value = LAYOSVT.get_i128(&row, 0);
				let next_value = current_value.saturating_add(1);

				if current_value == next_value {
					return_error!(sequence_exhausted(Type::Int16));
				}

				LAYOSVT.set_i128(&mut row, 0, next_value);
				tx.set(key, row)?;
				Ok(next_value)
			}
			None => {
				let result = default.unwrap_or(1i128);
				let mut new_row = LAYOSVT.allocate_row();
				LAYOSVT.set_i128(&mut new_row, 0, result);
				tx.set(key, new_row)?;
				Ok(result)
			}
		})
	}

	pub(crate) fn set(txn: &mut impl CommandTransaction, key: &EncodedKey, value: i128) -> crate::Result<()> {
		txn.with_single_command(|tx| {
			let mut row = match tx.get(key)? {
				Some(row) => row.row,
				None => LAYOSVT.allocate_row(),
			};
			LAYOSVT.set_i128(&mut row, 0, value);
			tx.set(key, row)?;
			Ok(())
		})
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::{
		EncodedKey,
		diagnostic::sequence::sequence_exhausted,
		interface::{SingleVersionCommandTransaction, SingleVersionQueryTransaction, SingleVersionRow},
	};
	use reifydb_engine::test_utils::create_test_command_transaction;
	use reifydb_type::Type;

	use crate::sequence::generator::i128::{GeneratorI128, LAYOSVT};

	#[test]
	fn test_ok() {
		let mut txn = create_test_command_transaction();
		for expected in 1..1000 {
			let got = GeneratorI128::next(&mut txn, &EncodedKey::new("sequence"), None).unwrap();
			assert_eq!(got, expected);
		}

		txn.with_single_query(|tx| {
			let mut single: Vec<SingleVersionRow> = tx.scan()?.collect();
			assert_eq!(single.len(), 2);

			single.pop().unwrap();
			let single = single.pop().unwrap();
			assert_eq!(single.key, EncodedKey::new("sequence"));
			assert_eq!(LAYOSVT.get_i128(&single.row, 0), 999);

			Ok(())
		})
		.unwrap();
	}

	#[test]
	fn test_exhaustion() {
		let mut txn = create_test_command_transaction();

		let mut row = LAYOSVT.allocate_row();
		LAYOSVT.set_i128(&mut row, 0, i128::MAX);

		txn.with_single_command(|tx| tx.set(&EncodedKey::new("sequence"), row)).unwrap();

		let err = GeneratorI128::next(&mut txn, &EncodedKey::new("sequence"), None).unwrap_err();
		assert_eq!(err.diagnostic(), sequence_exhausted(Type::Int16));
	}

	#[test]
	fn test_default() {
		let mut txn = create_test_command_transaction();

		let got = GeneratorI128::next(&mut txn, &EncodedKey::new("sequence_with_default"), Some(100i128))
			.unwrap();
		assert_eq!(got, 100);

		let got = GeneratorI128::next(&mut txn, &EncodedKey::new("sequence_with_default"), Some(999i128))
			.unwrap();
		assert_eq!(got, 101);
	}
}
