// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::{HashMap, HashSet, hash_map::Entry};

use reifydb_catalog::catalog::Catalog;
use reifydb_codec::row::{
	bytes::{EncodedBytes, read_fingerprint},
	pod::EncodedPodRow,
	shape::{RowShape, fingerprint::RowShapeFingerprint},
	table::EncodedTableRowBuilder,
};
use reifydb_core::{
	error::{CoreError, diagnostic::index::primary_key_violation},
	interface::catalog::{id::IndexId, key::PrimaryKey, table::Table},
	internal_error,
	key::{any::TaggedKey, catalog::IndexEntryKey},
	sort::SortDirection,
	value::index::{encoded::EncodedIndexKey, shape::IndexShape},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{
	error::Error,
	fragment::Fragment,
	value::{
		date::Date,
		datetime::DateTime,
		duration::Duration,
		identity::IdentityId,
		row_number::RowNumber,
		time::Time,
		uuid::{Uuid4, Uuid7},
		value_type::ValueType,
	},
};

use crate::{Result, bulk_insert::coerce::with_row_note};

pub struct PrimaryKeyEncoder {
	shape: IndexShape,
	columns: Vec<(usize, ValueType)>,
}

impl PrimaryKeyEncoder {
	pub fn new(pk_def: &PrimaryKey, table: &Table) -> Result<Self> {
		if let Some(column) = pk_def
			.columns
			.iter()
			.find(|c| matches!(c.constraint.get_type().inner_type(), ValueType::Digest { .. }))
		{
			return Err(CoreError::PrimaryKeyDigestColumn {
				fragment: Fragment::internal(&column.name),
				column: column.name.clone(),
				ty: column.constraint.get_type(),
			}
			.into());
		}
		let types: Vec<ValueType> = pk_def.columns.iter().map(|c| c.constraint.get_type()).collect();
		let directions = vec![SortDirection::Asc; types.len()];
		let shape = IndexShape::new(&types, &directions)?;
		let columns = pk_def
			.columns
			.iter()
			.map(|pk_column| {
				let table_idx = table
					.columns
					.iter()
					.position(|c| c.id == pk_column.id)
					.expect("Primary key column not found in table");
				(table_idx, pk_column.constraint.get_type())
			})
			.collect();
		Ok(Self {
			shape,
			columns,
		})
	}

	pub fn key_columns(&self) -> Vec<usize> {
		self.columns.iter().map(|(index, _)| *index).collect()
	}

	pub fn encode(&self, shape: &RowShape, row: &[u8]) -> EncodedIndexKey {
		let mut index_key = self.shape.allocate_key();
		for (pk_idx, (table_idx, column_type)) in self.columns.iter().enumerate() {
			match column_type {
				ValueType::Boolean => {
					let val = shape.get::<bool>(row, *table_idx);
					self.shape.set_bool(&mut index_key, pk_idx, val);
				}
				ValueType::Int1 => {
					let val = shape.get::<i8>(row, *table_idx);
					self.shape.set_i8(&mut index_key, pk_idx, val);
				}
				ValueType::Int2 => {
					let val = shape.get::<i16>(row, *table_idx);
					self.shape.set_i16(&mut index_key, pk_idx, val);
				}
				ValueType::Int4 => {
					let val = shape.get::<i32>(row, *table_idx);
					self.shape.set_i32(&mut index_key, pk_idx, val);
				}
				ValueType::Int8 => {
					let val = shape.get::<i64>(row, *table_idx);
					self.shape.set_i64(&mut index_key, pk_idx, val);
				}
				ValueType::Int16 => {
					let val = shape.get::<i128>(row, *table_idx);
					self.shape.set_i128(&mut index_key, pk_idx, val);
				}
				ValueType::Uint1 => {
					let val = shape.get::<u8>(row, *table_idx);
					self.shape.set_u8(&mut index_key, pk_idx, val);
				}
				ValueType::Uint2 => {
					let val = shape.get::<u16>(row, *table_idx);
					self.shape.set_u16(&mut index_key, pk_idx, val);
				}
				ValueType::Uint4 => {
					let val = shape.get::<u32>(row, *table_idx);
					self.shape.set_u32(&mut index_key, pk_idx, val);
				}
				ValueType::Uint8 => {
					let val = shape.get::<u64>(row, *table_idx);
					self.shape.set_u64(&mut index_key, pk_idx, val);
				}
				ValueType::Uint16 => {
					let val = shape.get::<u128>(row, *table_idx);
					self.shape.set_u128(&mut index_key, pk_idx, val);
				}
				ValueType::Float4 => {
					let val = shape.get::<f32>(row, *table_idx);
					self.shape.set_f32(&mut index_key, pk_idx, val);
				}
				ValueType::Float8 => {
					let val = shape.get::<f64>(row, *table_idx);
					self.shape.set_f64(&mut index_key, pk_idx, val);
				}
				ValueType::Date => {
					let val = shape.get::<Date>(row, *table_idx);
					self.shape.set_date(&mut index_key, pk_idx, val);
				}
				ValueType::Time => {
					let val = shape.get::<Time>(row, *table_idx);
					self.shape.set_time(&mut index_key, pk_idx, val);
				}
				ValueType::DateTime => {
					let val = shape.get::<DateTime>(row, *table_idx);
					self.shape.set_datetime(&mut index_key, pk_idx, val);
				}
				ValueType::Duration => {
					let val = shape.get::<Duration>(row, *table_idx);
					self.shape.set_duration(&mut index_key, pk_idx, val);
				}
				ValueType::Uuid4 => {
					let val = shape.get::<Uuid4>(row, *table_idx);
					self.shape.set_uuid4(&mut index_key, pk_idx, val);
				}
				ValueType::Uuid7 => {
					let val = shape.get::<Uuid7>(row, *table_idx);
					self.shape.set_uuid7(&mut index_key, pk_idx, val);
				}
				ValueType::IdentityId => {
					let val = shape.get::<IdentityId>(row, *table_idx);
					self.shape.set_identity_id(&mut index_key, pk_idx, val);
				}
				ValueType::Option(_) => {
					self.shape.set_none(&mut index_key, pk_idx);
				}
				ValueType::Utf8
				| ValueType::Blob
				| ValueType::Decimal {
					..
				}
				| ValueType::DictionaryId
				| ValueType::Any
				| ValueType::List(_)
				| ValueType::Record(_)
				| ValueType::Tuple(_) => {
					unreachable!("CREATE PRIMARY KEY refuses a {column_type} key column");
				}
				ValueType::Digest {
					..
				} => {
					unreachable!("IndexShape::new rejects a Digest primary key column");
				}
			}
		}
		index_key
	}
}

pub fn get_primary_key(catalog: &Catalog, txn: &mut Transaction<'_>, table: &Table) -> Result<Option<PrimaryKey>> {
	if let Some(_pk_id) = catalog.get_table_pk_id(txn, table.id)? {
		catalog.find_primary_key(txn, table.id)
	} else {
		Ok(None)
	}
}

pub(crate) enum Violation<'a> {
	At(&'a Fragment),
	BulkRow(&'a str),
}

pub(crate) fn checked_primary_keys(
	txn: &mut Transaction<'_>,
	table: &Table,
	pk: &PrimaryKey,
	encoder: &PrimaryKeyEncoder,
	shape: &RowShape,
	rows: &[EncodedTableRowBuilder],
	violation: &Violation<'_>,
) -> Result<Vec<EncodedIndexKey>> {
	let mut seen = HashSet::with_capacity(rows.len());
	let mut keys = Vec::with_capacity(rows.len());
	let mut duplicate = None;
	for (index, row) in rows.iter().enumerate() {
		let key = encoder.encode(shape, row);
		if !seen.insert(key.clone()) {
			duplicate = Some(index);
			break;
		}
		keys.push(key);
	}
	let entries: Vec<TaggedKey> = keys
		.iter()
		.map(|key| IndexEntryKey::new(table.id, IndexId::primary(pk.id), key.clone()).into())
		.collect();
	let taken = txn.contains_many(&entries)?.into_iter().position(|taken| taken).or(duplicate);
	if let Some(index) = taken {
		let key_columns = pk.columns.iter().map(|c| c.name.clone()).collect();
		let fragment = match violation {
			Violation::At(fragment) => (*fragment).clone(),
			Violation::BulkRow(_) => Fragment::None,
		};
		let error = Error(Box::new(primary_key_violation(fragment, table.name.clone(), key_columns)));
		return Err(match violation {
			Violation::At(_) => error,
			Violation::BulkRow(source) => with_row_note(error, source, index),
		});
	}
	Ok(keys)
}

pub(crate) fn primary_key_index(
	table: &Table,
	pk: &PrimaryKey,
	keys: Vec<EncodedIndexKey>,
	row_numbers: &[RowNumber],
) -> Vec<(TaggedKey, EncodedBytes)> {
	keys.into_iter()
		.zip(row_numbers)
		.map(|(key, &row_number)| {
			(
				IndexEntryKey::new(table.id, IndexId::primary(pk.id), key).into(),
				EncodedPodRow::new(&u64::from(row_number).to_be_bytes()).into_bytes(),
			)
		})
		.collect()
}

pub(crate) struct PrimaryKeyIndex<'a> {
	pk_def: &'a PrimaryKey,
	encoder: Option<PrimaryKeyEncoder>,
	shapes: HashMap<RowShapeFingerprint, RowShape>,
	removes: Vec<(TaggedKey, Option<EncodedBytes>)>,
}

impl<'a> PrimaryKeyIndex<'a> {
	pub(crate) fn new(pk_def: &'a PrimaryKey) -> Self {
		Self {
			pk_def,
			encoder: None,
			shapes: HashMap::new(),
			removes: Vec::new(),
		}
	}

	pub(crate) fn remove(
		&mut self,
		catalog: &Catalog,
		txn: &mut Transaction<'_>,
		table: &Table,
		values: &EncodedBytes,
	) -> Result<()> {
		let fingerprint = read_fingerprint(values);
		let shape = match self.shapes.entry(fingerprint) {
			Entry::Occupied(entry) => entry.into_mut(),
			Entry::Vacant(entry) => {
				let shape = catalog.get_or_load_row_shape(fingerprint, txn)?.ok_or_else(|| {
					internal_error!(
						"Row shape with fingerprint {:?} not found for table {}",
						fingerprint,
						table.name
					)
				})?;
				entry.insert(shape)
			}
		};
		let encoder = match &mut self.encoder {
			Some(encoder) => encoder,
			None => self.encoder.insert(PrimaryKeyEncoder::new(self.pk_def, table)?),
		};
		let index_key = encoder.encode(shape, values);
		self.removes
			.push((IndexEntryKey::new(table.id, IndexId::primary(self.pk_def.id), index_key).into(), None));
		Ok(())
	}

	pub(crate) fn write(self, txn: &mut Transaction<'_>) -> Result<()> {
		txn.remove_many(self.removes)
	}
}
