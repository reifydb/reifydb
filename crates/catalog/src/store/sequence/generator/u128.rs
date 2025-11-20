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

static LAYOUT: Lazy<EncodedValuesLayout> = Lazy::new(|| EncodedValuesLayout::new(&[Type::Uint16]));

pub(crate) struct GeneratorU128 {}

impl GeneratorU128 {
	pub(crate) fn next(
		txn: &mut impl CommandTransaction,
		key: &EncodedKey,
		default: Option<u128>,
	) -> crate::Result<u128> {
		Self::next_batched(txn, key, default, 1)
	}

	pub(crate) fn next_batched(
		txn: &mut impl CommandTransaction,
		key: &EncodedKey,
		default: Option<u128>,
		incr: u128,
	) -> crate::Result<u128> {
		txn.with_single_command([key], |tx| match tx.get(key)? {
			Some(row) => {
				let mut row = row.values;
				let current_value = LAYOUT.get_u128(&row, 0);
				let next_value = current_value.saturating_add(incr);

				if current_value == next_value {
					return_error!(sequence_exhausted(Type::Uint16));
				}

				LAYOUT.set_u128(&mut row, 0, next_value);
				tx.set(key, row)?;
				Ok(next_value)
			}
			None => {
				let result = default.unwrap_or(1u128);
				let mut new_row = LAYOUT.allocate();
				LAYOUT.set_u128(&mut new_row, 0, result);
				tx.set(key, new_row)?;
				Ok(result)
			}
		})
	}

	pub(crate) fn set(txn: &mut impl CommandTransaction, key: &EncodedKey, value: u128) -> crate::Result<()> {
		txn.with_single_command([key], |tx| {
			let mut row = match tx.get(key)? {
				Some(row) => row.values,
				None => LAYOUT.allocate(),
			};
			LAYOUT.set_u128(&mut row, 0, value);
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
		interface::{SingleVersionCommandTransaction, SingleVersionQueryTransaction},
	};
	use reifydb_engine::test_utils::create_test_command_transaction;
	use reifydb_type::Type;

	use crate::store::sequence::generator::u128::{GeneratorU128, LAYOUT};

	#[test]
	fn test_ok() {
		let mut txn = create_test_command_transaction();
		for expected in 1..1000 {
			let got = GeneratorU128::next(&mut txn, &EncodedKey::new("sequence"), None).unwrap();
			assert_eq!(got, expected);
		}

		let key = EncodedKey::new("sequence");
		txn.with_single_query([&key], |tx| {
			let single = tx.get(&key)?.unwrap();
			assert_eq!(LAYOUT.get_u128(&single.values, 0), 999);
			Ok(())
		})
		.unwrap();
	}

	#[test]
	fn test_exhaustion() {
		let mut txn = create_test_command_transaction();

		let mut row = LAYOUT.allocate();
		LAYOUT.set_u128(&mut row, 0, u128::MAX);

		let key = EncodedKey::new("sequence");
		txn.with_single_command([&key], |tx| tx.set(&key, row)).unwrap();

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

	#[test]
	fn test_batched_ok() {
		let mut txn = create_test_command_transaction();

		// Test incrementing by 50000
		for i in 0..20 {
			let expected = 1 + (i * 50000);
			let got = GeneratorU128::next_batched(
				&mut txn,
				&EncodedKey::new("sequence_by_50000"),
				None,
				50000,
			)
			.unwrap();
			assert_eq!(got, expected);
		}

		let key = EncodedKey::new("sequence_by_50000");
		txn.with_single_query([&key], |tx| {
			let single = tx.get(&key)?.unwrap();
			assert_eq!(LAYOUT.get_u128(&single.values, 0), 950001);
			Ok(())
		})
		.unwrap();

		// Test incrementing by 100000
		for i in 0..10 {
			let expected = 1 + (i * 100000);
			let got = GeneratorU128::next_batched(
				&mut txn,
				&EncodedKey::new("sequence_by_100000"),
				None,
				100000,
			)
			.unwrap();
			assert_eq!(got, expected);
		}
	}

	#[test]
	fn test_batched_exhaustion() {
		let mut txn = create_test_command_transaction();

		let mut row = LAYOUT.allocate();
		LAYOUT.set_u128(&mut row, 0, u128::MAX - 20000);

		let key = EncodedKey::new("sequence");
		txn.with_single_command([&key], |tx| tx.set(&key, row)).unwrap();

		// This should succeed (MAX - 20000 + 50000 saturates to MAX)
		let result = GeneratorU128::next_batched(&mut txn, &EncodedKey::new("sequence"), None, 50000).unwrap();
		assert_eq!(result, u128::MAX);

		// This should fail (already at MAX)
		let err = GeneratorU128::next_batched(&mut txn, &EncodedKey::new("sequence"), None, 50000).unwrap_err();
		assert_eq!(err.diagnostic(), sequence_exhausted(Type::Uint16));
	}

	#[test]
	fn test_batched_default() {
		let mut txn = create_test_command_transaction();

		let got = GeneratorU128::next_batched(
			&mut txn,
			&EncodedKey::new("sequence_with_default"),
			Some(100u128),
			50000,
		)
		.unwrap();
		assert_eq!(got, 100);

		let got = GeneratorU128::next_batched(
			&mut txn,
			&EncodedKey::new("sequence_with_default"),
			Some(999u128),
			50000,
		)
		.unwrap();
		assert_eq!(got, 50100);
	}
}
