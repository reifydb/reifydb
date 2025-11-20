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

static LAYOUT: Lazy<EncodedValuesLayout> = Lazy::new(|| EncodedValuesLayout::new(&[Type::Int1]));

pub(crate) struct GeneratorI8 {}

impl GeneratorI8 {
	pub(crate) fn next(
		txn: &mut impl CommandTransaction,
		key: &EncodedKey,
		default: Option<i8>,
	) -> crate::Result<i8> {
		Self::next_batched(txn, key, default, 1)
	}

	pub(crate) fn next_batched(
		txn: &mut impl CommandTransaction,
		key: &EncodedKey,
		default: Option<i8>,
		incr: i8,
	) -> crate::Result<i8> {
		txn.with_single_command([key], |tx| match tx.get(key)? {
			Some(row) => {
				let mut row = row.values;
				let current_value = LAYOUT.get_i8(&row, 0);
				let next_value = current_value.saturating_add(incr);

				if current_value == next_value {
					return_error!(sequence_exhausted(Type::Int1));
				}

				LAYOUT.set_i8(&mut row, 0, next_value);
				tx.set(key, row)?;
				Ok(next_value)
			}
			None => {
				let result = default.unwrap_or(1i8);
				let mut new_row = LAYOUT.allocate();
				LAYOUT.set_i8(&mut new_row, 0, result);
				tx.set(key, new_row)?;
				Ok(result)
			}
		})
	}

	pub(crate) fn set(txn: &mut impl CommandTransaction, key: &EncodedKey, value: i8) -> crate::Result<()> {
		txn.with_single_command([key], |tx| {
			let mut row = match tx.get(key)? {
				Some(row) => row.values,
				None => LAYOUT.allocate(),
			};
			LAYOUT.set_i8(&mut row, 0, value);
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

	use crate::store::sequence::generator::i8::{GeneratorI8, LAYOUT};

	#[test]
	fn test_ok() {
		let mut txn = create_test_command_transaction();
		for expected in 1..100 {
			let got = GeneratorI8::next(&mut txn, &EncodedKey::new("sequence"), None).unwrap();
			assert_eq!(got, expected);
		}

		let key = EncodedKey::new("sequence");
		txn.with_single_query([&key], |tx| {
			let single = tx.get(&key)?.unwrap();
			assert_eq!(LAYOUT.get_i8(&single.values, 0), 99);
			Ok(())
		})
		.unwrap();
	}

	#[test]
	fn test_exhaustion() {
		let mut txn = create_test_command_transaction();

		let mut row = LAYOUT.allocate();
		LAYOUT.set_i8(&mut row, 0, i8::MAX);

		let key = EncodedKey::new("sequence");
		txn.with_single_command([&key], |tx| tx.set(&key, row)).unwrap();

		let err = GeneratorI8::next(&mut txn, &EncodedKey::new("sequence"), None).unwrap_err();
		assert_eq!(err.diagnostic(), sequence_exhausted(Type::Int1));
	}

	#[test]
	fn test_default() {
		let mut txn = create_test_command_transaction();

		let got = GeneratorI8::next(&mut txn, &EncodedKey::new("sequence_with_default"), Some(10i8)).unwrap();
		assert_eq!(got, 10);

		let got = GeneratorI8::next(&mut txn, &EncodedKey::new("sequence_with_default"), Some(99i8)).unwrap();
		assert_eq!(got, 11);
	}

	#[test]
	fn test_batched_ok() {
		let mut txn = create_test_command_transaction();

		// Test incrementing by 5
		for i in 0..20 {
			let expected = 1 + (i * 5);
			let got = GeneratorI8::next_batched(&mut txn, &EncodedKey::new("sequence_by_5"), None, 5)
				.unwrap();
			assert_eq!(got, expected);
		}

		let key = EncodedKey::new("sequence_by_5");
		txn.with_single_query([&key], |tx| {
			let single = tx.get(&key)?.unwrap();
			assert_eq!(LAYOUT.get_i8(&single.values, 0), 96);
			Ok(())
		})
		.unwrap();

		// Test incrementing by 10
		for i in 0..10 {
			let expected = 1 + (i * 10);
			let got = GeneratorI8::next_batched(&mut txn, &EncodedKey::new("sequence_by_10"), None, 10)
				.unwrap();
			assert_eq!(got, expected);
		}
	}

	#[test]
	fn test_batched_exhaustion() {
		let mut txn = create_test_command_transaction();

		let mut row = LAYOUT.allocate();
		LAYOUT.set_i8(&mut row, 0, i8::MAX - 2);

		let key = EncodedKey::new("sequence");
		txn.with_single_command([&key], |tx| tx.set(&key, row)).unwrap();

		// This should succeed (MAX - 2 + 5 saturates to MAX)
		let result = GeneratorI8::next_batched(&mut txn, &EncodedKey::new("sequence"), None, 5).unwrap();
		assert_eq!(result, i8::MAX);

		// This should fail (already at MAX)
		let err = GeneratorI8::next_batched(&mut txn, &EncodedKey::new("sequence"), None, 5).unwrap_err();
		assert_eq!(err.diagnostic(), sequence_exhausted(Type::Int1));
	}

	#[test]
	fn test_batched_default() {
		let mut txn = create_test_command_transaction();

		let got = GeneratorI8::next_batched(&mut txn, &EncodedKey::new("sequence_with_default"), Some(10i8), 5)
			.unwrap();
		assert_eq!(got, 10);

		let got = GeneratorI8::next_batched(&mut txn, &EncodedKey::new("sequence_with_default"), Some(99i8), 5)
			.unwrap();
		assert_eq!(got, 15);
	}
}
