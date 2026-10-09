// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, hash::Hash, sync::LazyLock};

use arrow_array::{ArrayRef, make_array};
use arrow_row::{RowConverter, Rows, SortField};
use postcard::{from_bytes, to_stdvec};
use reifydb_codec::row::{
	bytes::{EncodedBytes, RowBuilder},
	shape::{RowFamily, RowShape, RowShapeField},
};
use reifydb_value::{
	Result,
	error::{Diagnostic, Error, IntoDiagnostic},
	fragment::Fragment,
	reifydb_assertions,
	value::{
		Value, blob::Blob, column_view::ColumnView, constraint::Constraint, partition::Partition,
		value_type::ValueType,
	},
};

use crate::{
	interface::catalog::{column::Column, object::ObjectId},
	value::{batch::decode_cells, column::builder::ColumnBuilder},
};

static REGISTRY_SHAPE: LazyLock<RowShape> =
	LazyLock::new(|| RowShape::new(RowFamily::Pod, vec![RowShapeField::unconstrained("values", ValueType::Blob)]));

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NoneCell {
	Untyped,
	Typed,
}

#[derive(Debug, Default)]
pub struct BatchPartitions {
	pub rows: Vec<Partition>,
	pub distinct: Vec<(Partition, Vec<Value>)>,
}

pub fn partitions_of_batch(keys: &[ColumnView<'_>], none: NoneCell) -> BatchPartitions {
	let rows = keys.first().map_or(0, ColumnView::len);
	match converted_rows(keys) {
		Some(converted) => assign(keys, none, rows, |row| converted.row(row)),
		None => assign(keys, none, rows, |row| {
			to_stdvec(&cell_values(keys, none, row)).expect("value postcard is total")
		}),
	}
}

fn converted_rows(keys: &[ColumnView<'_>]) -> Option<Rows> {
	let fields = keys.iter().map(|key| SortField::new(key.array().data_type().clone())).collect();
	let arrays: Vec<ArrayRef> = keys.iter().map(|key| make_array(key.array().to_data())).collect();
	RowConverter::new(fields).ok()?.convert_columns(&arrays).ok()
}

fn assign<K: Hash + Eq>(
	keys: &[ColumnView<'_>],
	none: NoneCell,
	rows: usize,
	key_of: impl Fn(usize) -> K,
) -> BatchPartitions {
	let mut groups: HashMap<K, Partition> = HashMap::new();
	let mut distinct = Vec::new();
	let mut partitions = Vec::with_capacity(rows);
	for row in 0..rows {
		let partition = *groups.entry(key_of(row)).or_insert_with(|| {
			let values = cell_values(keys, none, row);
			let partition = Partition::of(&values);
			distinct.push((partition, values));
			partition
		});
		partitions.push(partition);
	}
	BatchPartitions {
		rows: partitions,
		distinct,
	}
}

fn cell_values(keys: &[ColumnView<'_>], none: NoneCell, row: usize) -> Vec<Value> {
	keys.iter()
		.map(|key| match (key.get_value(row), none) {
			(
				Value::None {
					..
				},
				NoneCell::Untyped,
			) => Value::none(),
			(value, _) => value,
		})
		.collect()
}

fn registry_blob(values: &[Value]) -> Value {
	Value::Blob(Blob::from(to_stdvec(values).expect("value postcard is total")))
}

pub fn registry_row(values: &[Value]) -> EncodedBytes {
	let mut row = REGISTRY_SHAPE.allocate_pod();
	REGISTRY_SHAPE.set_value(&mut row, 0, &registry_blob(values));
	row.freeze_bytes()
}

pub fn registry_matches(row: &[u8], values: &[Value]) -> bool {
	REGISTRY_SHAPE.get_value(row, 0) == registry_blob(values)
}

pub fn registry_values(row: &[u8]) -> Vec<Value> {
	match REGISTRY_SHAPE.get_value(row, 0) {
		Value::Blob(blob) => from_bytes(blob.as_bytes()).expect("partition registry values must decode"),
		other => panic!("partition registry row holds {other:?} where a blob is required"),
	}
}

pub fn partitions_of_rows(shape: &RowShape, indices: &[usize], rows: &[EncodedBytes]) -> Result<Vec<Partition>> {
	let columns = indices
		.iter()
		.map(|&index| {
			let field = &shape.fields()[index];
			let mut builder = ColumnBuilder::with_capacity(field.constraint.get_type(), rows.len());
			if let Some(Constraint::Dictionary(dictionary, _)) = field.constraint.constraint() {
				builder.set_dictionary_id(*dictionary);
			}
			decode_cells(&mut builder, &field.name, shape, index, rows)?;
			Ok(builder.finish(&field.name))
		})
		.collect::<Result<Vec<_>>>()?;
	let views = columns.iter().map(ColumnView::try_from).collect::<Result<Vec<_>>>()?;
	let partitions = partitions_of_batch(&views, NoneCell::Untyped).rows;
	reifydb_assertions! {
		for (row, partition) in rows.iter().zip(&partitions) {
			let values: Vec<Value> = indices.iter().map(|&index| shape.get_value(row, index)).collect();
			assert_eq!(
				Partition::of(&values),
				*partition,
				"a partition decoded from stored rows must hash the cells the row holds"
			);
		}
	}
	Ok(partitions)
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

	#[error(
		"partition column changed by a pre-insert hook on object {object}: the row no longer hashes to its partition"
	)]
	PartitionColumnChangedByHook {
		object: ObjectId,
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

			PartitionError::PartitionColumnChangedByHook {
				object,
			} => Diagnostic {
				code: "PART_006".to_string(),
				rql: None,
				message: format!(
					"partition column changed by a pre-insert hook on object {}: the row no longer hashes to its partition",
					object
				),
				column: None,
				fragment: Fragment::None,
				label: Some("partition column change rejected".to_string()),
				help: Some(
					"a pre-insert hook must not change a partition column, since the row's partition is fixed before hooks run"
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
