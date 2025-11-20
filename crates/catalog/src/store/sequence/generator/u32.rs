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

static LAYOUT: Lazy<EncodedValuesLayout> = Lazy::new(|| EncodedValuesLayout::new(&[Type::Uint4]));

pub(crate) struct GeneratorU32 {}

impl GeneratorU32 {
	pub(crate) fn next(
		txn: &mut impl CommandTransaction,
		key: &EncodedKey,
		default: Option<u32>,
	) -> crate::Result<u32> {
		Self::next_batched(txn, key, default, 1)
	}

	pub(crate) fn next_batched(
		txn: &mut impl CommandTransaction,
		key: &EncodedKey,
		default: Option<u32>,
		incr: u32,
	) -> crate::Result<u32> {
		txn.with_single_command([key], |tx| match tx.get(key)? {
			Some(row) => {
				let mut row = row.values;
				let current_value = LAYOUT.get_u32(&row, 0);
				let next_value = current_value.saturating_add(incr);

				if current_value == next_value {
					return_error!(sequence_exhausted(Type::Uint4));
				}

				LAYOUT.set_u32(&mut row, 0, next_value);
				tx.set(key, row)?;
				Ok(next_value)
			}
			None => {
				let result = default.unwrap_or(1u32);
				let mut new_row = LAYOUT.allocate();
				LAYOUT.set_u32(&mut new_row, 0, result);
				tx.set(key, new_row)?;
				Ok(result)
			}
		})
	}

	pub(crate) fn set(txn: &mut impl CommandTransaction, key: &EncodedKey, value: u32) -> crate::Result<()> {
		txn.with_single_command([key], |tx| {
			let mut row = match tx.get(key)? {
				Some(row) => row.values,
				None => LAYOUT.allocate(),
			};
			LAYOUT.set_u32(&mut row, 0, value);
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

	use crate::store::sequence::generator::u32::{GeneratorU32, LAYOUT};

	#[test]
	fn test_ok() {
		let mut txn = create_test_command_transaction();
		for expected in 1..1000 {
			let got = GeneratorU32::next(&mut txn, &EncodedKey::new("sequence"), None).unwrap();
			assert_eq!(got, expected);
		}

		let key = EncodedKey::new("sequence");
		txn.with_single_query([&key], |tx| {
			let single = tx.get(&key)?.unwrap();
			assert_eq!(LAYOUT.get_u32(&single.values, 0), 999);
			Ok(())
		})
		.unwrap();
	}

	#[test]
	fn test_exhaustion() {
		let mut txn = create_test_command_transaction();

		let mut row = LAYOUT.allocate();
		LAYOUT.set_u32(&mut row, 0, u32::MAX);

		let key = EncodedKey::new("sequence");
		txn.with_single_command([&key], |tx| tx.set(&key, row)).unwrap();

		let err = GeneratorU32::next(&mut txn, &EncodedKey::new("sequence"), None).unwrap_err();
		assert_eq!(err.diagnostic(), sequence_exhausted(Type::Uint4));
	}

	#[test]
	fn test_default() {
		let mut txn = create_test_command_transaction();

		let got =
			GeneratorU32::next(&mut txn, &EncodedKey::new("sequence_with_default"), Some(100u32)).unwrap();
		assert_eq!(got, 100);

		let got =
			GeneratorU32::next(&mut txn, &EncodedKey::new("sequence_with_default"), Some(999u32)).unwrap();
		assert_eq!(got, 101);
	}

	#[test]
	fn test_batched_ok() {
		let mut txn = create_test_command_transaction();

		// Test incrementing by 500
		for i in 0..20 {
			let expected = 1 + (i * 500);
			let got = GeneratorU32::next_batched(&mut txn, &EncodedKey::new("sequence_by_500"), None, 500)
				.unwrap();
			assert_eq!(got, expected);
		}

		let key = EncodedKey::new("sequence_by_500");
		txn.with_single_query([&key], |tx| {
			let single = tx.get(&key)?.unwrap();
			assert_eq!(LAYOUT.get_u32(&single.values, 0), 9501);
			Ok(())
		})
		.unwrap();

		// Test incrementing by 1000
		for i in 0..10 {
			let expected = 1 + (i * 1000);
			let got =
				GeneratorU32::next_batched(&mut txn, &EncodedKey::new("sequence_by_1000"), None, 1000)
					.unwrap();
			assert_eq!(got, expected);
		}
	}

	#[test]
	fn test_batched_exhaustion() {
		let mut txn = create_test_command_transaction();

		let mut row = LAYOUT.allocate();
		LAYOUT.set_u32(&mut row, 0, u32::MAX - 200);

		let key = EncodedKey::new("sequence");
		txn.with_single_command([&key], |tx| tx.set(&key, row)).unwrap();

		// This should succeed (MAX - 200 + 500 saturates to MAX)
		let result = GeneratorU32::next_batched(&mut txn, &EncodedKey::new("sequence"), None, 500).unwrap();
		assert_eq!(result, u32::MAX);

		// This should fail (already at MAX)
		let err = GeneratorU32::next_batched(&mut txn, &EncodedKey::new("sequence"), None, 500).unwrap_err();
		assert_eq!(err.diagnostic(), sequence_exhausted(Type::Uint4));
	}

	#[test]
	fn test_batched_default() {
		let mut txn = create_test_command_transaction();

		let got = GeneratorU32::next_batched(
			&mut txn,
			&EncodedKey::new("sequence_with_default"),
			Some(100u32),
			500,
		)
		.unwrap();
		assert_eq!(got, 100);

		let got = GeneratorU32::next_batched(
			&mut txn,
			&EncodedKey::new("sequence_with_default"),
			Some(999u32),
			500,
		)
		.unwrap();
		assert_eq!(got, 600);
	}
}
