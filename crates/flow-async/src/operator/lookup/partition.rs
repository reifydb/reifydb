// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	interface::catalog::column::Column, internal_err, partition::partition_of, value::column::cast::cast_value,
};
use reifydb_value::{
	Result,
	value::{Value, partition::Partition, value_type::ValueType},
};

use crate::operator::host::HostContext;

pub fn lookup_partition(
	host: &mut dyn HostContext,
	columns: &[Column],
	partition_by: &[String],
	values: &[Value],
) -> Result<Option<Partition>> {
	if partition_by.len() != values.len() {
		return internal_err!(
			"lookup hashes {} values against {} partition columns",
			values.len(),
			partition_by.len()
		);
	}
	let mut hashed = Vec::with_capacity(values.len());
	for (name, value) in partition_by.iter().zip(values) {
		if matches!(value, Value::None { .. }) {
			return Ok(None);
		}
		let Some(column) = columns.iter().find(|column| column.name == *name) else {
			return internal_err!("lookup partition column {} is not a column of the right side", name);
		};
		let target = match column.constraint.get_type() {
			ValueType::Option(inner) => *inner,
			other => other,
		};
		let value = cast_value(value.clone(), &target)?;
		match column.dictionary_id {
			Some(dictionary) => match host.dictionary_find(dictionary, &value)? {
				Some(id) => hashed.push(id.to_value()),
				None => return Ok(None),
			},
			None => hashed.push(value),
		}
	}
	Ok(Some(partition_of(columns, partition_by, &hashed)))
}
