// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::shape::RowShape;
use reifydb_value::{
	error::{Diagnostic, Error, IntoDiagnostic},
	fragment::Fragment,
	reifydb_assertions,
	value::{Value, partition::Partition, value_type::ValueType},
};

use crate::interface::catalog::{column::Column, object::ObjectId};

pub fn partition_values(shape: &RowShape, row: &[u8], indices: &[usize]) -> Vec<Value> {
	indices.iter().map(|&i| shape.get_value(row, i)).collect()
}

#[cfg_attr(not(reifydb_assertions), allow(unused_variables))]
#[allow(clippy::disallowed_methods)]
pub fn partition_of(columns: &[Column], partition_by: &[String], values: &[Value]) -> Partition {
	reifydb_assertions! {
		assert_eq!(
			partition_by.len(),
			values.len(),
			"one value per partition column, otherwise the dictionary check skips columns"
		);
		for (name, value) in partition_by.iter().zip(values) {
			let column = columns
				.iter()
				.find(|c| c.name == *name)
				.expect("partition column must exist (validated during planning)");
			assert!(
				column.dictionary_id.is_none() || matches!(value, Value::DictionaryId(_) | Value::None { .. }),
				"partition column '{}' is dictionary-encoded but was hashed from {:?}: stored rows hash the \
				 entry id, so a plain value lands in a different partition",
				name,
				value
			);
		}
	}
	Partition::of(values)
}

pub fn partition_col_indices(columns: &[Column], partition_by: &[String]) -> Vec<usize> {
	partition_by
		.iter()
		.map(|pb| {
			columns.iter()
				.position(|c| c.name == *pb)
				.expect("partition column must exist (validated during planning)")
		})
		.collect()
}

#[derive(Debug, thiserror::Error)]
pub enum PartitionError {
	#[error("cannot change partition column via UPDATE on object {object}: partition columns are immutable")]
	ImmutablePartitionColumn {
		object: ObjectId,
	},

	#[error(
		"partition hash collision on object {object}: hash {hash:032x} maps to two distinct partition value tuples"
	)]
	PartitionHashCollision {
		object: ObjectId,
		hash: u128,
	},

	#[error("cannot partition by column `{column}`: a {ty} value cannot be a partition key")]
	DigestPartitionColumn {
		column: String,
		ty: ValueType,
		fragment: Fragment,
	},
}

impl IntoDiagnostic for PartitionError {
	fn into_diagnostic(self) -> Diagnostic {
		match self {
			PartitionError::ImmutablePartitionColumn {
				object,
			} => Diagnostic {
				code: "PART_002".to_string(),
				rql: None,
				message: format!(
					"cannot change partition column via UPDATE on object {}: partition columns are immutable",
					object
				),
				column: None,
				fragment: Fragment::None,
				label: Some("partition column change rejected".to_string()),
				help: Some(
					"partition columns determine a row's physical location and cannot be updated; delete and re-insert the row instead"
						.to_string(),
				),
				notes: vec![],
				cause: None,
				operator_chain: None,
			},

			PartitionError::PartitionHashCollision {
				object,
				hash,
			} => Diagnostic {
				code: "PART_003".to_string(),
				rql: None,
				message: format!(
					"partition hash collision on object {}: hash {:032x} maps to two distinct partition value tuples",
					object, hash
				),
				column: None,
				fragment: Fragment::None,
				label: Some("128-bit hash collision".to_string()),
				help: Some(
					"two distinct partition value tuples produced the same 128-bit hash; this is astronomically unlikely and points to a hashing bug or data corruption, report it as a bug"
						.to_string(),
				),
				notes: vec![],
				cause: None,
				operator_chain: None,
			},

			PartitionError::DigestPartitionColumn {
				column,
				ty,
				fragment,
			} => Diagnostic {
				code: "PART_005".to_string(),
				rql: None,
				message: format!("cannot partition by column `{}`: a {} value cannot be a partition key", column, ty),
				column: None,
				fragment,
				label: Some("digest partition column".to_string()),
				help: Some(
					"a digest has no key encoding and no equality, so it cannot address a partition; partition by a column of another type"
						.to_string(),
				),
				notes: vec![],
				cause: None,
				operator_chain: None,
			},
		}
	}
}

impl From<PartitionError> for Error {
	fn from(err: PartitionError) -> Self {
		Error(Box::new(err.into_diagnostic()))
	}
}

#[cfg(test)]
#[allow(clippy::disallowed_methods)]
mod tests {
	use std::slice::from_ref;

	use reifydb_value::value::{
		constraint::TypeConstraint,
		dictionary::{DictionaryEntryId, DictionaryId},
	};

	use super::*;
	use crate::interface::catalog::{column::ColumnIndex, id::ColumnId};

	fn pool(dictionary_id: Option<DictionaryId>) -> Vec<Column> {
		vec![Column {
			id: ColumnId(1),
			name: "pool".to_string(),
			constraint: TypeConstraint::unconstrained(ValueType::Utf8),
			properties: vec![],
			index: ColumnIndex(0),
			auto_increment: false,
			dictionary_id,
		}]
	}

	#[test]
	fn a_dictionary_partition_column_hashes_its_entry_id() {
		// The helper must hash the stored id unchanged, otherwise it disagrees with the rows it addresses.
		let id = DictionaryEntryId::U4(7).to_value();
		let partition = partition_of(&pool(Some(DictionaryId(1))), &["pool".to_string()], from_ref(&id));
		assert_eq!(partition, Partition::of(&[id]));
	}

	#[test]
	fn a_plain_partition_column_hashes_its_plain_value() {
		// The check must only guard dictionary columns, otherwise every plain partitioned write fails.
		let value = Value::Utf8("aa".to_string());
		let partition = partition_of(&pool(None), &["pool".to_string()], from_ref(&value));
		assert_eq!(partition, Partition::of(&[value]));
	}

	#[test]
	#[cfg(reifydb_assertions)]
	#[should_panic(expected = "is dictionary-encoded but was hashed from")]
	fn a_plain_value_on_a_dictionary_partition_column_is_refused() {
		// A site hashing the plain value puts the row in another partition than its stored twins, so it must
		// fail loudly.
		partition_of(&pool(Some(DictionaryId(1))), &["pool".to_string()], &[Value::Utf8("aa".to_string())]);
	}
}
