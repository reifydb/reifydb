// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use once_cell::sync::Lazy;
use reifydb_core::{
	EncodedKey,
	diagnostic::sequence::sequence_exhausted,
	interface::{CommandTransaction, SingleVersionCommandTransaction, SingleVersionQueryTransaction},
	return_error,
	value::encoded::EncodedValuesLayout,
};
use reifydb_type::Type;

static LAYOUT: Lazy<EncodedValuesLayout> = Lazy::new(|| EncodedValuesLayout::new(&[Type::Uint1]));

pub(crate) struct GeneratorU8 {}

impl GeneratorU8 {
	pub(crate) fn next(
		txn: &mut impl CommandTransaction,
		key: &EncodedKey,
		default: Option<u8>,
	) -> crate::Result<u8> {
		txn.with_single_command(|tx| match tx.get(key)? {
			Some(row) => {
				let mut row = row.values;
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
				let result = default.unwrap_or(1u8);
				let mut new_row = LAYOUT.allocate();
				LAYOUT.set_u8(&mut new_row, 0, result);
				tx.set(key, new_row)?;
				Ok(result)
			}
		})
	}

	pub(crate) fn set(txn: &mut impl CommandTransaction, key: &EncodedKey, value: u8) -> crate::Result<()> {
		txn.with_single_command(|tx| {
			let mut row = match tx.get(key)? {
				Some(row) => row.values,
				None => LAYOUT.allocate(),
			};
			LAYOUT.set_u8(&mut row, 0, value);
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
		interface::{SingleVersionCommandTransaction, SingleVersionQueryTransaction, SingleVersionValues},
	};
	use reifydb_engine::test_utils::create_test_command_transaction;
	use reifydb_type::Type;

	use crate::store::sequence::generator::u8::{GeneratorU8, LAYOUT};

	#[test]
	fn test_ok() {
		let mut txn = create_test_command_transaction();
		for expected in 1..100 {
			let got = GeneratorU8::next(&mut txn, &EncodedKey::new("sequence"), None).unwrap();
			assert_eq!(got, expected);
		}

		txn.with_single_query(|tx| {
			let mut single: Vec<SingleVersionValues> = tx.scan()?.collect();
			assert_eq!(single.len(), 2);

			single.pop().unwrap();
			let single = single.pop().unwrap();
			assert_eq!(single.key, EncodedKey::new("sequence"));
			assert_eq!(LAYOUT.get_u8(&single.values, 0), 99);

			Ok(())
		})
		.unwrap();
	}

	#[test]
	fn test_exhaustion() {
		let mut txn = create_test_command_transaction();

		let mut row = LAYOUT.allocate();
		LAYOUT.set_u8(&mut row, 0, u8::MAX);

		txn.with_single_command(|tx| tx.set(&EncodedKey::new("sequence"), row)).unwrap();

		let err = GeneratorU8::next(&mut txn, &EncodedKey::new("sequence"), None).unwrap_err();
		assert_eq!(err.diagnostic(), sequence_exhausted(Type::Uint1));
	}

	#[test]
	fn test_default() {
		let mut txn = create_test_command_transaction();

		let got = GeneratorU8::next(&mut txn, &EncodedKey::new("sequence_with_default"), Some(100u8)).unwrap();
		assert_eq!(got, 100);

		let got = GeneratorU8::next(&mut txn, &EncodedKey::new("sequence_with_default"), Some(200u8)).unwrap();
		assert_eq!(got, 101);
	}
}
