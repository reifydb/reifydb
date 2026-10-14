// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::{HashMap, HashSet, hash_map::Entry};

use arrow_array::ArrayRef;
use arrow_schema::FieldRef;
use reifydb_core::{
	interface::catalog::column::Column,
	internal_err,
	partition::partition_of,
	value::column::cast::{cast_column_data, cast_value, convert::TargetConvert},
};
use reifydb_value::{
	Result,
	fragment::Fragment,
	value::{
		Value,
		column_view::ColumnView,
		dictionary::{DictionaryEntryId, DictionaryId},
		partition::Partition,
		value_type::ValueType,
	},
};

use crate::operator::host::HostContext;

#[derive(Clone)]
pub enum RowKey {
	Missing,
	Unmatched,
	Found(Partition),
	Values(Vec<Value>),
}

#[derive(Default)]
pub struct DictionaryMemo {
	found: HashMap<(DictionaryId, Value), Option<DictionaryEntryId>>,
}

impl DictionaryMemo {
	fn find(
		&mut self,
		host: &mut dyn HostContext,
		dictionary: DictionaryId,
		value: Value,
	) -> Result<Option<DictionaryEntryId>> {
		match self.found.entry((dictionary, value)) {
			Entry::Occupied(found) => Ok(*found.get()),
			Entry::Vacant(missing) => {
				let id = host.dictionary_find(dictionary, &missing.key().1)?;
				Ok(*missing.insert(id))
			}
		}
	}

	fn find_many(
		&mut self,
		host: &mut dyn HostContext,
		dictionary: DictionaryId,
		values: &[Value],
	) -> Result<Vec<Option<DictionaryEntryId>>> {
		let mut seen = HashSet::new();
		let missing: Vec<Value> = values
			.iter()
			.filter(|value| {
				!self.found.contains_key(&(dictionary, (*value).clone()))
					&& seen.insert((*value).clone())
			})
			.cloned()
			.collect();
		if !missing.is_empty() {
			let ids = host.dictionary_find_many(dictionary, &missing)?;
			if ids.len() != missing.len() {
				return internal_err!(
					"dictionary find answered {} ids for {} values",
					ids.len(),
					missing.len()
				);
			}
			self.found.extend(missing.into_iter().map(|value| (dictionary, value)).zip(ids));
		}
		Ok(values.iter().map(|value| self.found[&(dictionary, value.clone())]).collect())
	}
}

struct PartitionSlot {
	target: ValueType,
	dictionary: Option<DictionaryId>,
}

pub struct PartitionLookup<'a> {
	columns: &'a [Column],
	partition_by: &'a [String],
	slots: Vec<Option<PartitionSlot>>,
}

impl<'a> PartitionLookup<'a> {
	pub fn new(columns: &'a [Column], partition_by: &'a [String]) -> Self {
		let slots = partition_by
			.iter()
			.map(|name| {
				columns.iter().find(|column| column.name == *name).map(|column| PartitionSlot {
					target: match column.constraint.get_type() {
						ValueType::Option(inner) => *inner,
						other => other,
					},
					dictionary: column.dictionary_id,
				})
			})
			.collect();
		Self {
			columns,
			partition_by,
			slots,
		}
	}

	pub fn partition(
		&self,
		host: &mut dyn HostContext,
		memo: &mut DictionaryMemo,
		values: &[Value],
	) -> Result<Option<Partition>> {
		if self.partition_by.len() != values.len() {
			return internal_err!(
				"lookup hashes {} values against {} partition columns",
				values.len(),
				self.partition_by.len()
			);
		}
		let mut hashed = Vec::with_capacity(values.len());
		for ((name, slot), value) in self.partition_by.iter().zip(&self.slots).zip(values) {
			if matches!(value, Value::None { .. }) {
				return Ok(None);
			}
			let Some(slot) = slot else {
				return internal_err!(
					"lookup partition column {} is not a column of the right side",
					name
				);
			};
			let value = cast_value(value.clone(), &slot.target)?;
			match slot.dictionary {
				Some(dictionary) => match memo.find(host, dictionary, value)? {
					Some(id) => hashed.push(id.to_value()),
					None => return Ok(None),
				},
				None => hashed.push(value),
			}
		}
		Ok(Some(partition_of(self.columns, self.partition_by, &hashed)))
	}

	pub fn keys(
		&self,
		host: &mut dyn HostContext,
		memo: &mut DictionaryMemo,
		columns: &[(FieldRef, ArrayRef)],
		rows: usize,
	) -> Result<Vec<RowKey>> {
		let views: Vec<ColumnView> = columns.iter().map(ColumnView::try_from).collect::<Result<_>>()?;
		let full: Vec<usize> = (0..rows).filter(|&row| views.iter().all(|view| view.is_defined(row))).collect();
		let mut keys = vec![RowKey::Missing; rows];
		if full.is_empty() {
			return Ok(keys);
		}
		match self.found(host, memo, &views, &full)? {
			Some(found) => {
				for (&row, key) in full.iter().zip(found) {
					keys[row] = key;
				}
			}
			None => {
				for &row in &full {
					keys[row] =
						RowKey::Values(views.iter().map(|view| view.get_value(row)).collect());
				}
			}
		}
		Ok(keys)
	}

	fn found(
		&self,
		host: &mut dyn HostContext,
		memo: &mut DictionaryMemo,
		views: &[ColumnView],
		full: &[usize],
	) -> Result<Option<Vec<RowKey>>> {
		if self.partition_by.len() != views.len() {
			return Ok(None);
		}
		let mut hashed: Vec<Vec<Value>> = full.iter().map(|_| Vec::with_capacity(views.len())).collect();
		let mut matched = vec![true; full.len()];
		for ((name, slot), view) in self.partition_by.iter().zip(&self.slots).zip(views) {
			let Some(slot) = slot else {
				return Ok(None);
			};
			let convert = TargetConvert {
				target: None,
			};
			let Ok(cast) =
				cast_column_data(convert, view, slot.target.clone(), || Fragment::internal(name))
			else {
				return Ok(None);
			};
			let cast = ColumnView::try_from(&cast)?;
			let values: Vec<Value> = full.iter().map(|&row| cast.get_value(row)).collect();
			match slot.dictionary {
				Some(dictionary) => {
					let ids = memo.find_many(host, dictionary, &values)?;
					for ((key, hit), id) in hashed.iter_mut().zip(matched.iter_mut()).zip(ids) {
						match id {
							Some(id) => key.push(id.to_value()),
							None => *hit = false,
						}
					}
				}
				None => {
					for (key, value) in hashed.iter_mut().zip(values) {
						key.push(value);
					}
				}
			}
		}
		Ok(Some(hashed
			.into_iter()
			.zip(matched)
			.map(|(key, hit)| match hit {
				true => RowKey::Found(partition_of(self.columns, self.partition_by, &key)),
				false => RowKey::Unmatched,
			})
			.collect()))
	}
}
