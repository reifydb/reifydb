// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use once_cell::sync::Lazy;
use reifydb_core::{
	EncodedKey, diagnostic::sequence::sequence_exhausted, return_error, value::encoded::EncodedValuesLayout,
};
use reifydb_transaction::StandardCommandTransaction;
use reifydb_type::Type;

macro_rules! impl_generator {
	(
		module: $mod_name:ident,
		name: $generator:ident,
		type: $prim:ty,
		type_enum: $type_enum:expr,
		getter: $getter:ident,
		setter: $setter:ident,
		start_value: $start:expr,
		max_value: $max:expr
	) => {
		pub(crate) mod $mod_name {
			use super::*;

			pub(crate) static LAYOUT: Lazy<EncodedValuesLayout> =
				Lazy::new(|| EncodedValuesLayout::new(&[$type_enum]));

			pub(crate) struct $generator {}

			impl $generator {
				pub(crate) async fn next(
					txn: &mut StandardCommandTransaction,
					key: &EncodedKey,
					default: Option<$prim>,
				) -> crate::Result<$prim> {
					Self::next_batched(txn, key, default, 1).await
				}

				pub(crate) async fn next_batched(
					txn: &mut StandardCommandTransaction,
					key: &EncodedKey,
					default: Option<$prim>,
					incr: $prim,
				) -> crate::Result<$prim> {
					let mut tx = txn.begin_single_command([key]).await?;
					let result = match tx.get(key).await? {
						Some(row) => {
							let mut row = row.values;
							let current_value = LAYOUT.$getter(&row, 0);
							let next_value = current_value.saturating_add(incr);

							if current_value == next_value {
								return_error!(sequence_exhausted($type_enum));
							}

							LAYOUT.$setter(&mut row, 0, next_value);
							tx.set(key, row)?;
							next_value
						}
						None => match default {
							Some(value) => {
								// When default is provided, initialize to that value
								// (ignore incr) This allows resuming a sequence
								// from a specific point
								let mut new_row = LAYOUT.allocate();
								LAYOUT.$setter(&mut new_row, 0, value);
								tx.set(key, new_row)?;
								value
							}
							None => {
								// When no default, allocate 'incr' contiguous IDs
								// starting from start_value
								let first = $start;
								let last = first.saturating_add(incr.saturating_sub(1));

								if first == last && incr > 1 {
									return_error!(sequence_exhausted($type_enum));
								}

								let mut new_row = LAYOUT.allocate();
								LAYOUT.$setter(&mut new_row, 0, last);
								tx.set(key, new_row)?;
								last
							}
						},
					};
					tx.commit().await?;
					Ok(result)
				}

				pub(crate) async fn set(
					txn: &mut StandardCommandTransaction,
					key: &EncodedKey,
					value: $prim,
				) -> crate::Result<()> {
					let mut tx = txn.begin_single_command([key]).await?;
					let mut row = match tx.get(key).await? {
						Some(row) => row.values,
						None => LAYOUT.allocate(),
					};
					LAYOUT.$setter(&mut row, 0, value);
					tx.set(key, row)?;
					tx.commit().await?;
					Ok(())
				}
			}

			#[cfg(test)]
			mod tests {
				use reifydb_core::{EncodedKey, diagnostic::sequence::sequence_exhausted};
				use reifydb_engine::test_utils::create_test_command_transaction;
				use reifydb_type::Type;

				use super::{LAYOUT, $generator};

				#[tokio::test]
				async fn test_ok() {
					let mut txn = create_test_command_transaction().await;
					let iterations =
						999u32.min(($max as u128).saturating_sub($start as u128) as u32);
					let count = ($start as u128).saturating_add(iterations as u128) as $prim;
					for expected in $start..count {
						let got =
							$generator::next(&mut txn, &EncodedKey::new("sequence"), None)
								.await
								.unwrap();
						assert_eq!(got, expected);
					}

					let key = EncodedKey::new("sequence");
					let mut tx = txn.begin_single_query([&key]).await.unwrap();
					let single = tx.get(&key).await.unwrap().unwrap();
					let final_val = ($start as u128)
						.saturating_add((iterations.saturating_sub(1)) as u128)
						as $prim;
					assert_eq!(LAYOUT.$getter(&single.values, 0), final_val);
				}

				#[tokio::test]
				async fn test_exhaustion() {
					let mut txn = create_test_command_transaction().await;

					let mut row = LAYOUT.allocate();
					LAYOUT.$setter(&mut row, 0, $max);

					let key = EncodedKey::new("sequence");
					txn.with_single_command([&key], |tx| tx.set(&key, row)).await.unwrap();

					let err = $generator::next(&mut txn, &EncodedKey::new("sequence"), None)
						.await
						.unwrap_err();
					assert_eq!(err.diagnostic(), sequence_exhausted($type_enum));
				}

				#[tokio::test]
				async fn test_default() {
					let mut txn = create_test_command_transaction().await;

					let default_val = ($start as u32).saturating_add(99).min($max as u32) as $prim;
					let got = $generator::next(
						&mut txn,
						&EncodedKey::new("sequence_with_default"),
						Some(default_val),
					)
					.await
					.unwrap();
					assert_eq!(got, default_val);

					let next_default =
						($start as u32).saturating_add(998).min($max as u32) as $prim;
					let got = $generator::next(
						&mut txn,
						&EncodedKey::new("sequence_with_default"),
						Some(next_default),
					)
					.await
					.unwrap();
					assert_eq!(
						got,
						(default_val as u32).saturating_add(1).min($max as u32) as $prim
					);
				}

				#[tokio::test]
				async fn test_batched_ok() {
					let mut txn = create_test_command_transaction().await;

					// Determine appropriate batch size and iteration count based on type range
					let type_range = ($max as u128).saturating_sub($start as u128);
					let (batch_size_1, iterations_1, batch_size_2, iterations_2) =
						if type_range < 200_000 {
							// For small types (u8, i8, u16, i16), use smaller batches
							let bs1 = (5u32.min(type_range as u32 / 40)) as $prim;
							let bs2 = (10u32.min(type_range as u32 / 20)) as $prim;
							(
								bs1,
								20u32.min((type_range / (bs1 as u128)) as u32),
								bs2,
								10u32.min((type_range / (bs2 as u128)) as u32),
							)
						} else {
							// For larger types, use the original batch sizes
							let bs1 = 5000u32 as $prim;
							let bs2 = 10000u32 as $prim;
							(bs1, 20, bs2, 10)
						};

					// Test batch allocation by batch_size_1
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
						.await
						.unwrap();
						assert_eq!(got, expected, "Call {} should return {}", i + 1, expected);
					}

					let key = EncodedKey::new("sequence_by_5000");
					let mut tx = txn.begin_single_query([&key]).await.unwrap();
					let single = tx.get(&key).await.unwrap().unwrap();
					let final_val = ($start as u128)
						.saturating_add((batch_size_1 as u128) * (iterations_1 as u128))
						.saturating_sub(1) as $prim;
					assert_eq!(LAYOUT.$getter(&single.values, 0), final_val);

					// Test batch allocation by batch_size_2
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
						.await
						.unwrap();
						assert_eq!(got, expected, "Call {} should return {}", i + 1, expected);
					}
				}

				#[tokio::test]
				async fn test_batched_exhaustion() {
					let mut txn = create_test_command_transaction().await;

					let mut row = LAYOUT.allocate();
					// Choose batch size and initial value that will cause saturation to MAX
					let batch_size_val =
						5000u32.min((($max as u128).saturating_sub($start as u128) / 2) as u32);
					let batch_size = batch_size_val as $prim;
					let initial_val =
						(($max as u128).saturating_sub((batch_size_val * 2) as u128)) as $prim;
					LAYOUT.$setter(&mut row, 0, initial_val);

					let key = EncodedKey::new("sequence");
					txn.with_single_command([&key], |tx| tx.set(&key, row)).await.unwrap();

					// This should succeed (initial + batch_size saturates to something less than
					// MAX)
					let result = $generator::next_batched(
						&mut txn,
						&EncodedKey::new("sequence"),
						None,
						batch_size,
					)
					.await
					.unwrap();
					// For some types this might not reach MAX yet, so we just check it increased
					assert!(result > initial_val);

					// Keep incrementing until we hit MAX
					loop {
						match $generator::next_batched(
							&mut txn,
							&EncodedKey::new("sequence"),
							None,
							batch_size,
						)
						.await
						{
							Ok(val) => {
								if val == $max {
									break;
								}
							}
							Err(_) => break,
						}
					}

					// Now we should be at MAX, next call should fail
					let err = $generator::next_batched(
						&mut txn,
						&EncodedKey::new("sequence"),
						None,
						batch_size,
					)
					.await
					.unwrap_err();
					assert_eq!(err.diagnostic(), sequence_exhausted($type_enum));
				}

				#[tokio::test]
				async fn test_batched_default() {
					let mut txn = create_test_command_transaction().await;

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
					.await
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
					.await
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

// Generate all unsigned integer generators
impl_generator!(
	module: u8,
	name: GeneratorU8,
	type: u8,
	type_enum: Type::Uint1,
	getter: get_u8,
	setter: set_u8,
	start_value: 1u8,
	max_value: u8::MAX
);

impl_generator!(
	module: u16,
	name: GeneratorU16,
	type: u16,
	type_enum: Type::Uint2,
	getter: get_u16,
	setter: set_u16,
	start_value: 1u16,
	max_value: u16::MAX
);

impl_generator!(
	module: u32,
	name: GeneratorU32,
	type: u32,
	type_enum: Type::Uint4,
	getter: get_u32,
	setter: set_u32,
	start_value: 1u32,
	max_value: u32::MAX
);

impl_generator!(
	module: u64,
	name: GeneratorU64,
	type: u64,
	type_enum: Type::Uint8,
	getter: get_u64,
	setter: set_u64,
	start_value: 1u64,
	max_value: u64::MAX
);

impl_generator!(
	module: u128,
	name: GeneratorU128,
	type: u128,
	type_enum: Type::Uint16,
	getter: get_u128,
	setter: set_u128,
	start_value: 1u128,
	max_value: u128::MAX
);

// Generate all signed integer generators
impl_generator!(
	module: i8,
	name: GeneratorI8,
	type: i8,
	type_enum: Type::Int1,
	getter: get_i8,
	setter: set_i8,
	start_value: 1i8,
	max_value: i8::MAX
);

impl_generator!(
	module: i16,
	name: GeneratorI16,
	type: i16,
	type_enum: Type::Int2,
	getter: get_i16,
	setter: set_i16,
	start_value: 1i16,
	max_value: i16::MAX
);

impl_generator!(
	module: i32,
	name: GeneratorI32,
	type: i32,
	type_enum: Type::Int4,
	getter: get_i32,
	setter: set_i32,
	start_value: 1i32,
	max_value: i32::MAX
);

impl_generator!(
	module: i64,
	name: GeneratorI64,
	type: i64,
	type_enum: Type::Int8,
	getter: get_i64,
	setter: set_i64,
	start_value: 1i64,
	max_value: i64::MAX
);

impl_generator!(
	module: i128,
	name: GeneratorI128,
	type: i128,
	type_enum: Type::Int16,
	getter: get_i128,
	setter: set_i128,
	start_value: 1i128,
	max_value: i128::MAX
);
