// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use once_cell::sync::Lazy;
use reifydb_core::interface::{
	CommandTransaction,
};
use reifydb_core::{
	diagnostic::sequence::sequence_exhausted, interface::{
		UnversionedCommandTransaction,
		UnversionedQueryTransaction,
	},
	return_error,
	row::EncodedRowLayout,
	EncodedKey,
	Type,
};

static LAYOUT: Lazy<EncodedRowLayout> =
	Lazy::new(|| EncodedRowLayout::new(&[Type::Int4]));

pub(crate) struct GeneratorI32 {}

impl GeneratorI32 {
	pub(crate) fn next(
		txn: &mut impl CommandTransaction,
		key: &EncodedKey,
		default: Option<i32>,
	) -> crate::Result<i32> {
		txn.with_unversioned_command(|tx| match tx.get(key)? {
			Some(unversioned_row) => {
				let mut row = unversioned_row.row;
				let current_value = LAYOUT.get_i32(&row, 0);
				let next_value =
					current_value.saturating_add(1);

				if current_value == next_value {
					return_error!(sequence_exhausted(
						Type::Int4
					));
				}

				LAYOUT.set_i32(&mut row, 0, next_value);
				tx.set(key, row)?;
				Ok(next_value)
			}
			None => {
				let result = default.unwrap_or(1i32);
				let mut new_row = LAYOUT.allocate_row();
				LAYOUT.set_i32(&mut new_row, 0, result);
				tx.set(key, new_row)?;
				Ok(result)
			}
		})
	}

	pub(crate) fn set(
		txn: &mut impl CommandTransaction,
		key: &EncodedKey,
		value: i32,
	) -> crate::Result<()> {
		txn.with_unversioned_command(|tx| {
			let mut row = match tx.get(key)? {
				Some(unversioned_row) => unversioned_row.row,
				None => LAYOUT.allocate_row(),
			};
			LAYOUT.set_i32(&mut row, 0, value);
			tx.set(key, row)?;
			Ok(())
		})
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::{
		interface::{
			Unversioned, UnversionedCommandTransaction,
			UnversionedQueryTransaction,
		}, result::error::diagnostic::sequence::sequence_exhausted,
		EncodedKey,
		Type,
	};
	use reifydb_transaction::test_utils::create_test_command_transaction;

	use crate::sequence::generator::i32::{GeneratorI32, LAYOUT};

	#[test]
	fn test_ok() {
		let mut txn = create_test_command_transaction();
		for expected in 1..1000 {
			let got = GeneratorI32::next(
				&mut txn,
				&EncodedKey::new("sequence"),
				None,
			)
			.unwrap();
			assert_eq!(got, expected);
		}

		txn.with_unversioned_query(|tx| {
			let mut unversioned: Vec<Unversioned> =
				tx.scan()?.collect();
			assert_eq!(unversioned.len(), 2);

			unversioned.pop().unwrap();
			let unversioned = unversioned.pop().unwrap();
			assert_eq!(
				unversioned.key,
				EncodedKey::new("sequence")
			);
			assert_eq!(LAYOUT.get_i32(&unversioned.row, 0), 999);

			Ok(())
		})
		.unwrap();
	}

	#[test]
	fn test_exhaustion() {
		let mut txn = create_test_command_transaction();

		let mut row = LAYOUT.allocate_row();
		LAYOUT.set_i32(&mut row, 0, i32::MAX);

		txn.with_unversioned_command(|tx| {
			tx.set(&EncodedKey::new("sequence"), row)
		})
		.unwrap();

		let err = GeneratorI32::next(
			&mut txn,
			&EncodedKey::new("sequence"),
			None,
		)
		.unwrap_err();
		assert_eq!(err.diagnostic(), sequence_exhausted(Type::Int4));
	}

	#[test]
	fn test_default() {
		let mut txn = create_test_command_transaction();

		let got = GeneratorI32::next(
			&mut txn,
			&EncodedKey::new("sequence_with_default"),
			Some(100i32),
		)
		.unwrap();
		assert_eq!(got, 100);

		let got = GeneratorI32::next(
			&mut txn,
			&EncodedKey::new("sequence_with_default"),
			Some(999i32),
		)
		.unwrap();
		assert_eq!(got, 101);
	}
}
