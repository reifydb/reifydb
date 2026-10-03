// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::{HashMap, hash_map::Entry};

use reifydb_core::{
	interface::catalog::column::Column, internal_err, partition::partition_of, value::column::cast::cast_value,
};
use reifydb_value::{
	Result,
	value::{
		Value,
		dictionary::{DictionaryEntryId, DictionaryId},
		partition::Partition,
		value_type::ValueType,
	},
};

use crate::operator::host::HostContext;

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
}
