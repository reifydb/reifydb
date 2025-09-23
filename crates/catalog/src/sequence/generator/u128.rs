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

static LAYOSVT: Lazy<EncodedRowLayout> = Lazy::new(|| EncodedRowLayout::new(&[Type::Uint16]));

pub(crate) struct GeneratorU128 {}

impl GeneratorU128 {
	pub(crate) fn next(
		txn: &mut impl CommandTransaction,
		key: &EncodedKey,
		default: Option<u128>,
	) -> crate::Result<u128> {
		txn.with_single_command(|tx| match tx.get(key)? {
			Some(row) => {
				let mut row = row.row;
				let current_value = LAYOSVT.get_u128(&row, 0);
				let next_value = current_value.saturating_add(1);

				if current_value == next_value {
					return_error!(sequence_exhausted(Type::Uint16));
				}

				LAYOSVT.set_u128(&mut row, 0, next_value);
				tx.set(key, row)?;
				Ok(next_value)
			}
			None => {
				let result = default.unwrap_or(1u128);
				let mut new_row = LAYOSVT.allocate_row();
				LAYOSVT.set_u128(&mut new_row, 0, result);
				tx.set(key, new_row)?;
				Ok(result)
			}
		})
	}

	pub(crate) fn set(txn: &mut impl CommandTransaction, key: &EncodedKey, value: u128) -> crate::Result<()> {
		txn.with_single_command(|tx| {
			let mut row = match tx.get(key)? {
				Some(row) => row.row,
				None => LAYOSVT.allocate_row(),
			};
			LAYOSVT.set_u128(&mut row, 0, value);
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

	use crate::sequence::generator::u128::{GeneratorU128, LAYOSVT};

	#[test]
	fn test_ok() {
		let mut txn = create_test_command_transaction();
		for expected in 1..1000 {
			let got = GeneratorU128::next(&mut txn, &EncodedKey::new("sequence"), None).unwrap();
			assert_eq!(got, expected);
		}

		txn.with_single_query(|tx| {
			let mut single: Vec<SingleVersionRow> = tx.scan()?.collect();
			assert_eq!(single.len(), 2);

			single.pop().unwrap();
			let single = single.pop().unwrap();
			assert_eq!(single.key, EncodedKey::new("sequence"));
			assert_eq!(LAYOSVT.get_u128(&single.row, 0), 999);

			Ok(())
		})
		.unwrap();
	}

	#[test]
	fn test_exhaustion() {
		let mut txn = create_test_command_transaction();

		let mut row = LAYOSVT.allocate_row();
		LAYOSVT.set_u128(&mut row, 0, u128::MAX);

		txn.with_single_command(|tx| tx.set(&EncodedKey::new("sequence"), row)).unwrap();

		let err = GeneratorU128::next(&mut txn, &EncodedKey::new("sequence"), None).unwrap_err();
		assert_eq!(err.diagnostic(), sequence_exhausted(Type::Uint16));
	}

	#[test]
	fn test_default() {
		let mut txn = create_test_command_transaction();

		let got = GeneratorU128::next(&mut txn, &EncodedKey::new("sequence_with_default"), Some(100u128))
			.unwrap();
		assert_eq!(got, 100);

		let got = GeneratorU128::next(&mut txn, &EncodedKey::new("sequence_with_default"), Some(999u128))
			.unwrap();
		assert_eq!(got, 101);
	}
}
