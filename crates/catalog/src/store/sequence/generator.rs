// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::{key::encoded::EncodedKey, row::pod::EncodedPodRow};
use reifydb_core::{error::CoreError, return_internal_error};
use reifydb_transaction::{
	single::write::SingleWriteTransaction,
	transaction::{Transaction, admin::AdminTransaction, command::CommandTransaction},
};
use reifydb_value::{Result as TxResult, value::value_type::ValueType};

pub trait SequenceTransaction: Send {
	fn begin_single_command<'a, I>(&self, keys: I) -> TxResult<SingleWriteTransaction<'_>>
	where
		I: IntoIterator<Item = &'a EncodedKey>;

	fn as_transaction(&mut self) -> Transaction<'_>;
}

impl SequenceTransaction for CommandTransaction {
	fn begin_single_command<'a, I>(&self, keys: I) -> TxResult<SingleWriteTransaction<'_>>
	where
		I: IntoIterator<Item = &'a EncodedKey>,
	{
		CommandTransaction::begin_single_command(self, keys)
	}

	fn as_transaction(&mut self) -> Transaction<'_> {
		Transaction::Command(self)
	}
}

impl SequenceTransaction for AdminTransaction {
	fn begin_single_command<'a, I>(&self, keys: I) -> TxResult<SingleWriteTransaction<'_>>
	where
		I: IntoIterator<Item = &'a EncodedKey>,
	{
		AdminTransaction::begin_single_command(self, keys)
	}

	fn as_transaction(&mut self) -> Transaction<'_> {
		Transaction::Admin(self)
	}
}

impl SequenceTransaction for Transaction<'_> {
	fn begin_single_command<'a, I>(&self, keys: I) -> TxResult<SingleWriteTransaction<'_>>
	where
		I: IntoIterator<Item = &'a EncodedKey>,
	{
		Transaction::begin_single_command(self, keys)
	}

	fn as_transaction(&mut self) -> Transaction<'_> {
		self.reborrow()
	}
}

macro_rules! impl_generator {
	(
		module: $mod_name:ident,
		name: $generator:ident,
		type: $prim:ty,
		type_enum: $type_enum:expr,
		start_value: $start:expr,
		max_value: $max:expr
	) => {
		pub(crate) mod $mod_name {
			use super::*;
			use crate::Result;

			pub(crate) const WIDTH: usize = size_of::<$prim>();

			pub(crate) fn encode(value: $prim) -> EncodedPodRow {
				EncodedPodRow::new(&value.to_be_bytes())
			}

			pub(crate) fn decode(row: &EncodedPodRow) -> Result<$prim> {
				let Ok(bytes) = <[u8; WIDTH]>::try_from(row.body()) else {
					return_internal_error!(
						"Sequence counter is {} bytes wide, expected {}. This indicates a corrupt or foreign sequence row.",
						row.len(),
						WIDTH
					)
				};
				Ok(<$prim>::from_be_bytes(bytes))
			}

			pub(crate) struct $generator {}

			impl $generator {
				pub(crate) fn next(
					txn: &mut impl SequenceTransaction,
					key: &EncodedKey,
					default: Option<$prim>,
				) -> Result<$prim> {
					Self::next_batched(txn, key, default, 1)
				}

				pub(crate) fn next_batched(
					txn: &mut impl SequenceTransaction,
					key: &EncodedKey,
					default: Option<$prim>,
					incr: $prim,
				) -> Result<$prim> {
					let mut tx = txn.begin_single_command([key])?;
					let result = match tx.get(key)? {
						Some(row) => {
							let current_value = decode(EncodedPodRow::view(&row.bytes))?;
							let next_value = current_value.saturating_add(incr);

							if current_value == next_value {
								return Err(CoreError::SequenceExhausted {
									value_type: $type_enum,
								}
								.into());
							}

							tx.set(key, encode(next_value).into_bytes())?;
							next_value
						}
						None => match default {
							Some(value) => {
								tx.set(key, encode(value).into_bytes())?;
								value
							}
							None => {
								let first = $start;
								let last = first.saturating_add(incr.saturating_sub(1));

								if first == last && incr > 1 {
									return Err(CoreError::SequenceExhausted {
										value_type: $type_enum,
									}
									.into());
								}

								tx.set(key, encode(last).into_bytes())?;
								last
							}
						},
					};
					tx.commit()?;
					Ok(result)
				}

				pub(crate) fn set(
					txn: &mut impl SequenceTransaction,
					key: &EncodedKey,
					value: $prim,
				) -> Result<()> {
					let mut tx = txn.begin_single_command([key])?;
					tx.set(key, encode(value).into_bytes())?;
					tx.commit()?;
					Ok(())
				}
			}

			#[cfg(test)]
			mod tests {
				use reifydb_codec::{key::encoded::EncodedKey, row::pod::EncodedPodRow};
				use reifydb_core::error::CoreError;
				use reifydb_test_harness::engine::create_test_admin_transaction;
				use reifydb_value::{error::IntoDiagnostic, value::value_type::ValueType};

				use super::{WIDTH, decode, encode, $generator};

				#[test]
				fn a_counter_round_trips_at_exactly_the_type_width() {
					// Encoder and decoder disagreeing on width or endianness re-issues ids that
					// were already handed out.
					let row = encode($max);
					assert_eq!(row.len(), WIDTH);
					assert_eq!(decode(&row).unwrap(), $max);
					assert_eq!(decode(&encode($start)).unwrap(), $start);
				}

				#[test]
				fn a_row_of_the_wrong_width_is_rejected_rather_than_misread() {
					// The 33-byte shape row this replaced decodes as a plausible counter and
					// silently rewinds id issuance.
					assert!(decode(&EncodedPodRow::new(&vec![0u8; WIDTH + 1])).is_err());
					assert!(decode(&EncodedPodRow::new(&vec![0u8; WIDTH - 1])).is_err());
					assert!(decode(&EncodedPodRow::new(&vec![0u8; 33])).is_err());
				}

				#[test]
				fn test_ok() {
					let mut txn = create_test_admin_transaction();
					let iterations =
						999u32.min(($max as u128).saturating_sub($start as u128) as u32);
					let count = ($start as u128).saturating_add(iterations as u128) as $prim;
					for expected in $start..count {
						let got =
							$generator::next(&mut txn, &EncodedKey::new("sequence"), None)
								.unwrap();
						assert_eq!(got, expected);
					}

					let key = EncodedKey::new("sequence");
					let mut tx = txn.begin_single_query([&key]).unwrap();
					let single = tx.get(&key).unwrap().unwrap();
					let final_val = ($start as u128)
						.saturating_add((iterations.saturating_sub(1)) as u128)
						as $prim;
					assert_eq!(decode(EncodedPodRow::view(&single.bytes)).unwrap(), final_val);
				}

				#[test]
				fn test_exhaustion() {
					let mut txn = create_test_admin_transaction();

					let key = EncodedKey::new("sequence");
					txn.with_single_command([&key], |tx| tx.set(&key, encode($max).into_bytes()))
						.unwrap();

					let err = $generator::next(&mut txn, &EncodedKey::new("sequence"), None)
						.unwrap_err();
					assert_eq!(
						err.diagnostic(),
						CoreError::SequenceExhausted {
							value_type: $type_enum
						}
						.into_diagnostic()
					);
				}

				#[test]
				fn testault() {
					let mut txn = create_test_admin_transaction();

					let default_val = ($start as u32).saturating_add(99).min($max as u32) as $prim;
					let got = $generator::next(
						&mut txn,
						&EncodedKey::new("sequence_with_default"),
						Some(default_val),
					)
					.unwrap();
					assert_eq!(got, default_val);

					let next_default =
						($start as u32).saturating_add(998).min($max as u32) as $prim;
					let got = $generator::next(
						&mut txn,
						&EncodedKey::new("sequence_with_default"),
						Some(next_default),
					)
					.unwrap();
					assert_eq!(
						got,
						(default_val as u32).saturating_add(1).min($max as u32) as $prim
					);
				}

				#[test]
				fn test_batched_ok() {
					let mut txn = create_test_admin_transaction();

					// Batch sizes are scaled to the type's range so narrow types are not
					// exhausted before the iteration count is reached.
					let type_range = ($max as u128).saturating_sub($start as u128);
					let (batch_size_1, iterations_1, batch_size_2, iterations_2) =
						if type_range < 200_000 {
							let bs1 = (5u32.min(type_range as u32 / 40)) as $prim;
							let bs2 = (10u32.min(type_range as u32 / 20)) as $prim;
							(
								bs1,
								20u32.min((type_range / (bs1 as u128)) as u32),
								bs2,
								10u32.min((type_range / (bs2 as u128)) as u32),
							)
						} else {
							let bs1 = 5000u32 as $prim;
							let bs2 = 10000u32 as $prim;
							(bs1, 20, bs2, 10)
						};

					for i in 0..iterations_1 {
						let expected = ($start as u128)
							.saturating_add((batch_size_1 as u128) * ((i as u128) + 1))
							.saturating_sub(1) as $prim;
						let got = $generator::next_batched(
							&mut txn,
							&EncodedKey::new("sequence_by_5000"),
							None,
							batch_size_1,
						)
						.unwrap();
						assert_eq!(got, expected, "Call {} should return {}", i + 1, expected);
					}

					let key = EncodedKey::new("sequence_by_5000");
					let mut tx = txn.begin_single_query([&key]).unwrap();
					let single = tx.get(&key).unwrap().unwrap();
					let final_val = ($start as u128)
						.saturating_add((batch_size_1 as u128) * (iterations_1 as u128))
						.saturating_sub(1) as $prim;
					assert_eq!(decode(EncodedPodRow::view(&single.bytes)).unwrap(), final_val);

					for i in 0..iterations_2 {
						let expected = ($start as u128)
							.saturating_add((batch_size_2 as u128) * ((i as u128) + 1))
							.saturating_sub(1) as $prim;
						let got = $generator::next_batched(
							&mut txn,
							&EncodedKey::new("sequence_by_10000"),
							None,
							batch_size_2,
						)
						.unwrap();
						assert_eq!(got, expected, "Call {} should return {}", i + 1, expected);
					}
				}

				#[test]
				fn test_batched_exhaustion() {
					let mut txn = create_test_admin_transaction();

					let batch_size_val =
						5000u32.min((($max as u128).saturating_sub($start as u128) / 2) as u32);
					let batch_size = batch_size_val as $prim;
					let initial_val =
						(($max as u128).saturating_sub((batch_size_val * 2) as u128)) as $prim;

					let key = EncodedKey::new("sequence");
					txn.with_single_command([&key], |tx| {
						tx.set(&key, encode(initial_val).into_bytes())
					})
					.unwrap();

					let result = $generator::next_batched(
						&mut txn,
						&EncodedKey::new("sequence"),
						None,
						batch_size,
					)
					.unwrap();
					// Narrow types may not reach MAX in one batch, so only monotonicity holds here.
					assert!(result > initial_val);

					loop {
						match $generator::next_batched(
							&mut txn,
							&EncodedKey::new("sequence"),
							None,
							batch_size,
						) {
							Ok(val) => {
								if val == $max {
									break;
								}
							}
							Err(_) => break,
						}
					}

					let err = $generator::next_batched(
						&mut txn,
						&EncodedKey::new("sequence"),
						None,
						batch_size,
					)
					.unwrap_err();
					assert_eq!(
						err.diagnostic(),
						CoreError::SequenceExhausted {
							value_type: $type_enum
						}
						.into_diagnostic()
					);
				}

				#[test]
				fn test_batched_default() {
					let mut txn = create_test_admin_transaction();

					let type_range = ($max as u128).saturating_sub($start as u128);
					let default_val =
						($start as u128).saturating_add(99.min(type_range / 4)) as $prim;
					let batch_size = (5000u32.min((type_range / 4) as u32)) as $prim;
					let got = $generator::next_batched(
						&mut txn,
						&EncodedKey::new("sequence_with_default"),
						Some(default_val),
						batch_size,
					)
					.unwrap();
					assert_eq!(got, default_val);

					let next_default =
						($start as u128).saturating_add(998.min(type_range / 3)) as $prim;
					let got = $generator::next_batched(
						&mut txn,
						&EncodedKey::new("sequence_with_default"),
						Some(next_default),
						batch_size,
					)
					.unwrap();
					assert_eq!(
						got,
						(default_val as u128)
							.saturating_add(batch_size as u128)
							.min($max as u128) as $prim
					);
				}
			}
		}
	};
}

impl_generator!(
	module: u8,
	name: GeneratorU8,
	type: u8,
	type_enum: ValueType::Uint1,
	start_value: 1u8,
	max_value: u8::MAX
);

impl_generator!(
	module: u16,
	name: GeneratorU16,
	type: u16,
	type_enum: ValueType::Uint2,
	start_value: 1u16,
	max_value: u16::MAX
);

impl_generator!(
	module: u32,
	name: GeneratorU32,
	type: u32,
	type_enum: ValueType::Uint4,
	start_value: 1u32,
	max_value: u32::MAX
);

impl_generator!(
	module: u64,
	name: GeneratorU64,
	type: u64,
	type_enum: ValueType::Uint8,
	start_value: 1u64,
	max_value: u64::MAX
);

impl_generator!(
	module: u128,
	name: GeneratorU128,
	type: u128,
	type_enum: ValueType::Uint16,
	start_value: 1u128,
	max_value: u128::MAX
);

impl_generator!(
	module: i8,
	name: GeneratorI8,
	type: i8,
	type_enum: ValueType::Int1,
	start_value: 1i8,
	max_value: i8::MAX
);

impl_generator!(
	module: i16,
	name: GeneratorI16,
	type: i16,
	type_enum: ValueType::Int2,
	start_value: 1i16,
	max_value: i16::MAX
);

impl_generator!(
	module: i32,
	name: GeneratorI32,
	type: i32,
	type_enum: ValueType::Int4,
	start_value: 1i32,
	max_value: i32::MAX
);

impl_generator!(
	module: i64,
	name: GeneratorI64,
	type: i64,
	type_enum: ValueType::Int8,
	start_value: 1i64,
	max_value: i64::MAX
);

impl_generator!(
	module: i128,
	name: GeneratorI128,
	type: i128,
	type_enum: ValueType::Int16,
	start_value: 1i128,
	max_value: i128::MAX
);
