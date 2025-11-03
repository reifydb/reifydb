// Copyright (c) reifydb.com 2025.
// This file is licensed under the AGPL-3.0-or-later, see license.md file.

use std::{collections::VecDeque, ops::Bound};

use reifydb_core::{CowVec, EncodedKey, interface::SingleVersionValues, value::encoded::EncodedValues};
use rusqlite::Statement;

mod commit;
mod contains;
mod get;
mod range;
mod range_rev;

pub use range::SingleVersionRangeIter;
pub use range_rev::SingleVersionRangeRevIter;

use crate::backend::result::SingleVersionIterResult;

/// Helper function to build single query template and determine parameter
/// count
pub(crate) fn build_single_query(
	start_bound: Bound<&EncodedKey>,
	end_bound: Bound<&EncodedKey>,
	order: &str, // "ASC" or "DESC"
) -> (&'static str, u8) {
	match (start_bound, end_bound) {
		(Bound::Unbounded, Bound::Unbounded) => match order {
			"ASC" => ("SELECT key, value FROM single ORDER BY key ASC LIMIT ?", 0),
			"DESC" => ("SELECT key, value FROM single ORDER BY key DESC LIMIT ?", 0),
			_ => unreachable!(),
		},
		(Bound::Included(_), Bound::Unbounded) => match order {
			"ASC" => ("SELECT key, value FROM single WHERE key >= ? ORDER BY key ASC LIMIT ?", 1),
			"DESC" => ("SELECT key, value FROM single WHERE key >= ? ORDER BY key DESC LIMIT ?", 1),
			_ => unreachable!(),
		},
		(Bound::Excluded(_), Bound::Unbounded) => match order {
			"ASC" => ("SELECT key, value FROM single WHERE key > ? ORDER BY key ASC LIMIT ?", 1),
			"DESC" => ("SELECT key, value FROM single WHERE key > ? ORDER BY key DESC LIMIT ?", 1),
			_ => unreachable!(),
		},
		(Bound::Unbounded, Bound::Included(_)) => match order {
			"ASC" => ("SELECT key, value FROM single WHERE key <= ? ORDER BY key ASC LIMIT ?", 1),
			"DESC" => ("SELECT key, value FROM single WHERE key <= ? ORDER BY key DESC LIMIT ?", 1),
			_ => unreachable!(),
		},
		(Bound::Unbounded, Bound::Excluded(_)) => match order {
			"ASC" => ("SELECT key, value FROM single WHERE key < ? ORDER BY key ASC LIMIT ?", 1),
			"DESC" => ("SELECT key, value FROM single WHERE key < ? ORDER BY key DESC LIMIT ?", 1),
			_ => unreachable!(),
		},
		(Bound::Included(_), Bound::Included(_)) => match order {
			"ASC" => (
				"SELECT key, value FROM single WHERE key >= ? AND key <= ? ORDER BY key ASC LIMIT ?",
				2,
			),
			"DESC" => (
				"SELECT key, value FROM single WHERE key >= ? AND key <= ? ORDER BY key DESC LIMIT ?",
				2,
			),
			_ => unreachable!(),
		},
		(Bound::Included(_), Bound::Excluded(_)) => match order {
			"ASC" => {
				("SELECT key, value FROM single WHERE key >= ? AND key < ? ORDER BY key ASC LIMIT ?", 2)
			}
			"DESC" => (
				"SELECT key, value FROM single WHERE key >= ? AND key < ? ORDER BY key DESC LIMIT ?",
				2,
			),
			_ => unreachable!(),
		},
		(Bound::Excluded(_), Bound::Included(_)) => match order {
			"ASC" => {
				("SELECT key, value FROM single WHERE key > ? AND key <= ? ORDER BY key ASC LIMIT ?", 2)
			}
			"DESC" => (
				"SELECT key, value FROM single WHERE key > ? AND key <= ? ORDER BY key DESC LIMIT ?",
				2,
			),
			_ => unreachable!(),
		},
		(Bound::Excluded(_), Bound::Excluded(_)) => match order {
			"ASC" => {
				("SELECT key, value FROM single WHERE key > ? AND key < ? ORDER BY key ASC LIMIT ?", 2)
			}
			"DESC" => {
				("SELECT key, value FROM single WHERE key > ? AND key < ? ORDER BY key DESC LIMIT ?", 2)
			}
			_ => unreachable!(),
		},
	}
}

/// Helper function to execute batched single range queries
pub(crate) fn execute_range_query(
	stmt: &mut Statement,
	start_bound: Bound<&EncodedKey>,
	end_bound: Bound<&EncodedKey>,
	batch_size: usize,
	param_count: u8,
	buffer: &mut VecDeque<SingleVersionIterResult>,
) -> usize {
	let mut count = 0;
	match param_count {
		0 => {
			let rows = stmt
				.query_map(rusqlite::params![batch_size], |values| {
					let key = EncodedKey::new(values.get::<_, Vec<u8>>(0)?);
					let value: Option<Vec<u8>> = values.get(1)?;
					match value {
						Some(val) => Ok(SingleVersionIterResult::Value(SingleVersionValues {
							key,
							values: EncodedValues(CowVec::new(val)),
						})),
						None => Ok(SingleVersionIterResult::Tombstone {
							key,
						}), // NULL value means deleted
					}
				})
				.unwrap();

			for result in rows {
				match result {
					Ok(iter_result) => {
						buffer.push_back(iter_result);
						count += 1;
					}
					Err(_) => break,
				}
			}
		}
		1 => {
			let param = match (start_bound, end_bound) {
				(Bound::Included(key), _) | (Bound::Excluded(key), _) => key.to_vec(),
				(_, Bound::Included(key)) | (_, Bound::Excluded(key)) => key.to_vec(),
				_ => unreachable!(),
			};
			let rows = stmt
				.query_map(rusqlite::params![param, batch_size], |values| {
					let key = EncodedKey::new(values.get::<_, Vec<u8>>(0)?);
					let value: Option<Vec<u8>> = values.get(1)?;
					match value {
						Some(val) => Ok(SingleVersionIterResult::Value(SingleVersionValues {
							key,
							values: EncodedValues(CowVec::new(val)),
						})),
						None => Ok(SingleVersionIterResult::Tombstone {
							key,
						}), // NULL value means deleted
					}
				})
				.unwrap();

			for result in rows {
				match result {
					Ok(iter_result) => {
						buffer.push_back(iter_result);
						count += 1;
					}
					Err(_) => break,
				}
			}
		}
		2 => {
			let start_param = match start_bound {
				Bound::Included(key) | Bound::Excluded(key) => key.to_vec(),
				_ => unreachable!(),
			};
			let end_param = match end_bound {
				Bound::Included(key) | Bound::Excluded(key) => key.to_vec(),
				_ => unreachable!(),
			};
			let rows = stmt
				.query_map(rusqlite::params![start_param, end_param, batch_size], |values| {
					let key = EncodedKey::new(values.get::<_, Vec<u8>>(0)?);
					let value: Option<Vec<u8>> = values.get(1)?;
					match value {
						Some(val) => Ok(SingleVersionIterResult::Value(SingleVersionValues {
							key,
							values: EncodedValues(CowVec::new(val)),
						})),
						None => Ok(SingleVersionIterResult::Tombstone {
							key,
						}), // NULL value means deleted
					}
				})
				.unwrap();

			for result in rows {
				match result {
					Ok(iter_result) => {
						buffer.push_back(iter_result);
						count += 1;
					}
					Err(_) => break,
				}
			}
		}
		_ => unreachable!(),
	}
	count
}
