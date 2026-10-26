// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::BTreeMap, slice::from_ref};

use reifydb_core::{
	interface::catalog::object::ObjectId,
	value::batch::{is_scalar, scalar_value},
};
use reifydb_evaluate::stack::{SymbolTable, Variable};
use reifydb_flow::compiler::child_plans;
use reifydb_rql::{nodes::PartitionKey, query::QueryPlan};
use reifydb_transaction::transaction::ScanLayout;
use reifydb_value::value::{
	Value,
	partition::{Partition, ScanPartitions},
	value_type::ValueType,
};

pub(crate) fn scan_partitions(
	plan: &QueryPlan,
	layout: ScanLayout,
	symbols: &SymbolTable,
) -> BTreeMap<ObjectId, ScanPartitions> {
	let mut partitions = BTreeMap::new();
	collect_partitions(plan, None, layout, symbols, &mut partitions);
	partitions
}

fn collect_partitions(
	plan: &QueryPlan,
	parameter: Option<ScanPartitions>,
	layout: ScanLayout,
	symbols: &SymbolTable,
	partitions: &mut BTreeMap<ObjectId, ScanPartitions>,
) {
	let mut record = |source: ObjectId, partition: ScanPartitions| {
		partitions
			.entry(source)
			.and_modify(|held: &mut ScanPartitions| {
				if *held != partition {
					*held = ScanPartitions::All;
				}
			})
			.or_insert(partition);
	};
	match plan {
		QueryPlan::TableScan(node) => record(
			ObjectId::Table(node.source.def().id),
			parameter.or(node.partition.map(ScanPartitions::One)).unwrap_or(ScanPartitions::All),
		),
		QueryPlan::ViewScan(node) => record(
			ObjectId::View(node.source.def().id()),
			match parameter {
				Some(ScanPartitions::Many(_)) | None => {
					node.partition.map_or(ScanPartitions::All, ScanPartitions::One)
				}
				Some(chosen) => chosen,
			},
		),
		QueryPlan::PartitionParameterScan(node) => collect_partitions(
			&node.scan,
			choose_partitions(&node.keys, layout, symbols)
				.map(ScanPartitions::Many)
				.or_else(|| choose_partition(&node.keys, layout, symbols).map(ScanPartitions::One)),
			layout,
			symbols,
			partitions,
		),
		other => {
			for child in child_plans(other) {
				collect_partitions(child, None, layout, symbols, partitions);
			}
		}
	}
}

pub(crate) fn choose_partition(keys: &[PartitionKey], layout: ScanLayout, symbols: &SymbolTable) -> Option<Partition> {
	match layout {
		ScanLayout::Row => {
			let values = keys
				.iter()
				.map(|key| resolve_partition_key(key, symbols))
				.collect::<Option<Vec<_>>>()?;
			Some(Partition::of(&values))
		}
		ScanLayout::Column => None,
	}
}

pub(crate) fn choose_partitions(
	keys: &[PartitionKey],
	layout: ScanLayout,
	symbols: &SymbolTable,
) -> Option<Vec<Partition>> {
	let (ScanLayout::Row, [key]) = (layout, keys) else {
		return None;
	};
	let values = match key {
		PartitionKey::In(items) => {
			items.iter().map(|item| resolve_partition_key(item, symbols)).collect::<Option<Vec<_>>>()?
		}
		PartitionKey::InList {
			variable,
			ty,
		} => list_items(variable.name(), symbols)?
			.into_iter()
			.filter(|value| !matches!(value, Value::None { .. }))
			.map(|value| exact_partition_value(value, ty))
			.collect::<Option<Vec<_>>>()?,
		PartitionKey::Constant(_)
		| PartitionKey::Variable {
			..
		} => return None,
	};
	let mut partitions: Vec<Partition> = values.iter().map(|value| Partition::of(from_ref(value))).collect();
	partitions.sort_unstable_by(|a, b| b.cmp(a));
	partitions.dedup();
	Some(partitions)
}

fn list_items(name: &str, symbols: &SymbolTable) -> Option<Vec<Value>> {
	let Some(Variable::Columns {
		batch,
	}) = symbols.get(name)
	else {
		return None;
	};
	if !is_scalar(batch) {
		return None;
	}
	match scalar_value(batch).ok()? {
		Value::List(items) => Some(items),
		Value::Any(inner) => match *inner {
			Value::List(items) => Some(items),
			_ => None,
		},
		_ => None,
	}
}

fn resolve_partition_key(key: &PartitionKey, symbols: &SymbolTable) -> Option<Value> {
	let (variable, ty) = match key {
		PartitionKey::Constant(value) => return Some(value.clone()),
		PartitionKey::Variable {
			variable,
			ty,
		} => (variable, ty),
		PartitionKey::In(_)
		| PartitionKey::InList {
			..
		} => return None,
	};
	let Some(Variable::Columns {
		batch,
	}) = symbols.get(variable.name())
	else {
		return None;
	};
	if !is_scalar(batch) {
		return None;
	}
	exact_partition_value(scalar_value(batch).ok()?, ty)
}

fn exact_partition_value(value: Value, ty: &ValueType) -> Option<Value> {
	if let Some(integer) = integer_of(&value) {
		return integer_as(integer, ty);
	}
	if value.get_type() != *ty {
		return None;
	}
	match value {
		Value::Boolean(_)
		| Value::Utf8(_)
		| Value::Date(_)
		| Value::DateTime(_)
		| Value::Time(_)
		| Value::Duration(_)
		| Value::Uuid4(_)
		| Value::Uuid7(_)
		| Value::IdentityId(_) => Some(value),
		_ => None,
	}
}

fn integer_of(value: &Value) -> Option<i128> {
	match value {
		Value::Int1(v) => Some(i128::from(*v)),
		Value::Int2(v) => Some(i128::from(*v)),
		Value::Int4(v) => Some(i128::from(*v)),
		Value::Int8(v) => Some(i128::from(*v)),
		Value::Int16(v) => Some(*v),
		Value::Uint1(v) => Some(i128::from(*v)),
		Value::Uint2(v) => Some(i128::from(*v)),
		Value::Uint4(v) => Some(i128::from(*v)),
		Value::Uint8(v) => Some(i128::from(*v)),
		Value::Uint16(v) => i128::try_from(*v).ok(),
		_ => None,
	}
}

fn integer_as(integer: i128, ty: &ValueType) -> Option<Value> {
	match ty {
		ValueType::Int1 => i8::try_from(integer).ok().map(Value::Int1),
		ValueType::Int2 => i16::try_from(integer).ok().map(Value::Int2),
		ValueType::Int4 => i32::try_from(integer).ok().map(Value::Int4),
		ValueType::Int8 => i64::try_from(integer).ok().map(Value::Int8),
		ValueType::Int16 => Some(Value::Int16(integer)),
		ValueType::Uint1 => u8::try_from(integer).ok().map(Value::Uint1),
		ValueType::Uint2 => u16::try_from(integer).ok().map(Value::Uint2),
		ValueType::Uint4 => u32::try_from(integer).ok().map(Value::Uint4),
		ValueType::Uint8 => u64::try_from(integer).ok().map(Value::Uint8),
		ValueType::Uint16 => u128::try_from(integer).ok().map(Value::Uint16),
		_ => None,
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::expression::VariableExpression;
	use reifydb_value::fragment::Fragment;

	use super::*;

	fn key(text: &str, ty: ValueType) -> PartitionKey {
		PartitionKey::Variable {
			variable: VariableExpression {
				fragment: Fragment::internal(text),
			},
			ty,
		}
	}

	fn symbols(values: Vec<(&str, Value)>) -> SymbolTable {
		let mut symbols = SymbolTable::new();
		for (name, value) in values {
			symbols.set(name.to_string(), Variable::scalar(value), false).expect("a fresh name");
		}
		symbols
	}

	fn chosen(keys: &[PartitionKey], values: Vec<(&str, Value)>) -> Option<Partition> {
		choose_partition(keys, ScanLayout::Row, &symbols(values))
	}

	#[test]
	fn a_value_of_another_integer_width_hashes_as_the_declared_type() {
		// The writer hashes the declared-type value, so any other width would name a partition no row is in.
		let keys = [key("$1", ValueType::Int4)];

		assert_eq!(chosen(&keys, vec![("1", Value::Int8(3))]), Some(Partition::of(&[Value::Int4(3)])));
		assert_eq!(chosen(&keys, vec![("1", Value::Uint1(3))]), Some(Partition::of(&[Value::Int4(3)])));
		assert_ne!(chosen(&keys, vec![("1", Value::Int8(3))]), Some(Partition::of(&[Value::Int8(3)])));
	}

	#[test]
	fn a_value_that_cannot_be_the_declared_type_exactly_is_not_pruned() {
		// Each of these must read every partition, otherwise a lossy or wrong-type hash drops matching rows.
		let int4 = [key("$1", ValueType::Int4)];
		let utf8 = [key("$1", ValueType::Utf8)];

		assert_eq!(chosen(&int4, vec![("1", Value::Int8(5_000_000_000))]), None);
		assert_eq!(chosen(&int4, vec![("1", Value::Utf8("3".to_string()))]), None);
		assert_eq!(chosen(&int4, vec![("1", Value::float8(3.0))]), None);
		assert_eq!(chosen(&int4, vec![("1", Value::none())]), None);
		assert_eq!(chosen(&int4, vec![]), None);
		assert_eq!(chosen(&utf8, vec![("1", Value::Int4(3))]), None);
	}

	#[test]
	fn same_type_keys_and_constants_hash_together_in_column_order() {
		// The partition is one hash over every key in declared order, so a constant must keep its slot.
		let keys = [PartitionKey::Constant(Value::Utf8("eu".to_string())), key("$tier", ValueType::Utf8)];

		assert_eq!(
			chosen(&keys, vec![("tier", Value::Utf8("gold".to_string()))]),
			Some(Partition::of(&[Value::Utf8("eu".to_string()), Value::Utf8("gold".to_string())]))
		);
	}

	#[test]
	fn a_column_layout_never_prunes() {
		// A column layout refuses partitioned row-store reads, so the scan there must stay unpruned.
		let keys = [key("$1", ValueType::Int4)];

		assert_eq!(choose_partition(&keys, ScanLayout::Column, &symbols(vec![("1", Value::Int4(3))])), None);
	}
}
