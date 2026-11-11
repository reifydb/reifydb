// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::BTreeMap, slice::from_ref};

use reifydb_catalog::catalog::Catalog;
use reifydb_core::{
	interface::catalog::object::ObjectId,
	internal_error,
	value::batch::{is_scalar, scalar_value},
};
use reifydb_evaluate::stack::{SymbolTable, Variable};
use reifydb_flow::compiler::child_plans;
use reifydb_rql::{nodes::PartitionKey, query::QueryPlan};
use reifydb_transaction::transaction::{ScanLayout, Transaction};
use reifydb_value::value::{
	Value,
	dictionary::DictionaryId,
	partition::{Partition, ScanPartitions},
	value_type::ValueType,
};

use crate::{Result, transaction::operation::dictionary::DictionaryOperations};

pub(crate) fn scan_partitions(
	plan: &QueryPlan,
	catalog: &Catalog,
	txn: &mut Transaction<'_>,
	symbols: &SymbolTable,
) -> Result<BTreeMap<ObjectId, ScanPartitions>> {
	let mut partitions = BTreeMap::new();
	collect_partitions(plan, None, catalog, txn, symbols, &mut partitions)?;
	Ok(partitions)
}

fn collect_partitions(
	plan: &QueryPlan,
	parameter: Option<ScanPartitions>,
	catalog: &Catalog,
	txn: &mut Transaction<'_>,
	symbols: &SymbolTable,
	partitions: &mut BTreeMap<ObjectId, ScanPartitions>,
) -> Result<()> {
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
		QueryPlan::PartitionParameterScan(node) => {
			let chosen = match choose_partitions(&node.keys, catalog, txn, symbols)? {
				Some(chosen) => chosen,
				None => choose_partition(&node.keys, catalog, txn, symbols)?,
			};
			let parameter = match chosen {
				ScanPartitions::All => None,
				chosen => Some(chosen),
			};
			collect_partitions(&node.scan, parameter, catalog, txn, symbols, partitions)?;
		}
		other => {
			for child in child_plans(other) {
				collect_partitions(child, None, catalog, txn, symbols, partitions)?;
			}
		}
	}
	Ok(())
}

enum Resolved {
	Value(Value),
	Unknown,
	Missing,
}

pub(crate) fn choose_partition(
	keys: &[PartitionKey],
	catalog: &Catalog,
	txn: &mut Transaction<'_>,
	symbols: &SymbolTable,
) -> Result<ScanPartitions> {
	if txn.layout() == ScanLayout::Column {
		return Ok(ScanPartitions::All);
	}
	let mut values = Vec::with_capacity(keys.len());
	let mut unknown = false;
	for key in keys {
		match resolve_key(key, catalog, txn, symbols)? {
			Resolved::Value(value) => values.push(value),
			Resolved::Unknown => unknown = true,
			Resolved::Missing => return Ok(ScanPartitions::Nothing),
		}
	}
	if unknown {
		return Ok(ScanPartitions::All);
	}
	Ok(ScanPartitions::One(Partition::of(&values)))
}

pub(crate) fn choose_partitions(
	keys: &[PartitionKey],
	catalog: &Catalog,
	txn: &mut Transaction<'_>,
	symbols: &SymbolTable,
) -> Result<Option<ScanPartitions>> {
	let (ScanLayout::Row, [key]) = (txn.layout(), keys) else {
		return Ok(None);
	};
	let (dictionary, key) = match key {
		PartitionKey::Dictionary {
			dictionary,
			key,
		} => (Some(*dictionary), key.as_ref()),
		other => (None, other),
	};
	let Some(values) = in_values(key, symbols) else {
		return Ok(None);
	};
	let values = match dictionary {
		Some(dictionary) => {
			let found: Vec<Value> =
				lookup(catalog, txn, dictionary, values)?.into_iter().flatten().collect();
			if found.is_empty() {
				return Ok(Some(ScanPartitions::Nothing));
			}
			found
		}
		None => values,
	};
	let mut partitions: Vec<Partition> = values.iter().map(|value| Partition::of(from_ref(value))).collect();
	partitions.sort_unstable_by(|a, b| b.cmp(a));
	partitions.dedup();
	Ok(Some(ScanPartitions::Many(partitions)))
}

fn in_values(key: &PartitionKey, symbols: &SymbolTable) -> Option<Vec<Value>> {
	match key {
		PartitionKey::In(items) => {
			items.iter().map(|item| resolve_partition_key(item, symbols)).collect::<Option<Vec<_>>>()
		}
		PartitionKey::InList {
			variable,
			ty,
		} => list_items(variable.name(), symbols)?
			.into_iter()
			.filter(|value| !matches!(value, Value::None { .. }))
			.map(|value| exact_partition_value(value, ty))
			.collect::<Option<Vec<_>>>(),
		PartitionKey::Constant(_)
		| PartitionKey::Variable {
			..
		}
		| PartitionKey::Dictionary {
			..
		} => None,
	}
}

fn resolve_key(
	key: &PartitionKey,
	catalog: &Catalog,
	txn: &mut Transaction<'_>,
	symbols: &SymbolTable,
) -> Result<Resolved> {
	let PartitionKey::Dictionary {
		dictionary,
		key,
	} = key
	else {
		return Ok(resolve_partition_key(key, symbols).map_or(Resolved::Unknown, Resolved::Value));
	};
	let Some(value) = resolve_partition_key(key, symbols) else {
		return Ok(Resolved::Unknown);
	};
	Ok(match lookup(catalog, txn, *dictionary, vec![value])?.pop().flatten() {
		Some(id) => Resolved::Value(id),
		None => Resolved::Missing,
	})
}

fn lookup(
	catalog: &Catalog,
	txn: &mut Transaction<'_>,
	dictionary: DictionaryId,
	values: Vec<Value>,
) -> Result<Vec<Option<Value>>> {
	let found = catalog.find_dictionary(txn, dictionary)?.ok_or_else(|| {
		internal_error!("dictionary {:?} of a partition key is not in the catalog", dictionary)
	})?;
	Ok(txn.find_in_dictionary(&found, &values)?.into_iter().map(|id| id.map(Value::DictionaryId)).collect())
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
		}
		| PartitionKey::Dictionary {
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
	use std::sync::Arc;

	use postcard::to_stdvec;
	use reifydb_catalog::catalog::{dictionary::DictionaryToCreate, namespace::NamespaceToCreate};
	use reifydb_codec::row::pod::EncodedPodRow;
	use reifydb_core::{
		expression::VariableExpression,
		interface::catalog::{dictionary::Dictionary, id::NamespaceId},
		key::catalog::DictionaryEntryKey,
	};
	use reifydb_runtime::context::transaction::TransactionContext;
	use reifydb_test_harness::engine::create_test_admin_transaction;
	use reifydb_transaction::transaction::{admin::AdminTransaction, query::QueryTransaction};
	use reifydb_value::{fragment::Fragment, util::hash::xxh3_128};

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

	fn chosen(keys: &[PartitionKey], values: Vec<(&str, Value)>) -> ScanPartitions {
		let mut admin = create_test_admin_transaction();
		choose_partition(keys, &Catalog::testing(), &mut Transaction::Admin(&mut admin), &symbols(values))
			.unwrap()
	}

	fn dictionary_fixture() -> (Catalog, AdminTransaction, Dictionary) {
		let catalog = Catalog::testing();
		let mut admin = create_test_admin_transaction();
		let namespace = catalog
			.create_namespace(
				&mut admin,
				NamespaceToCreate {
					namespace_fragment: None,
					name: "ns".to_string(),
					local_name: "ns".to_string(),
					parent_id: NamespaceId::ROOT,
					grpc: None,
					token: None,
				},
			)
			.unwrap();
		let dictionary = catalog
			.create_dictionary(
				&mut admin,
				DictionaryToCreate {
					name: Fragment::internal("pools"),
					namespace: namespace.id(),
					value_type: ValueType::Utf8,
					id_type: ValueType::Uint4,
				},
			)
			.unwrap();
		(catalog, admin, dictionary)
	}

	fn through(dictionary: &Dictionary, key: PartitionKey) -> PartitionKey {
		PartitionKey::Dictionary {
			dictionary: dictionary.id,
			key: Box::new(key),
		}
	}

	fn utf8(text: &str) -> Value {
		Value::Utf8(text.to_string())
	}

	#[test]
	fn a_value_of_another_integer_width_hashes_as_the_declared_type() {
		// The writer hashes the declared-type value, so any other width would name a partition no row is in.
		let keys = [key("$1", ValueType::Int4)];

		assert_eq!(
			chosen(&keys, vec![("1", Value::Int8(3))]),
			ScanPartitions::One(Partition::of(&[Value::Int4(3)]))
		);
		assert_eq!(
			chosen(&keys, vec![("1", Value::Uint1(3))]),
			ScanPartitions::One(Partition::of(&[Value::Int4(3)]))
		);
		assert_ne!(
			chosen(&keys, vec![("1", Value::Int8(3))]),
			ScanPartitions::One(Partition::of(&[Value::Int8(3)]))
		);
	}

	#[test]
	fn a_value_that_cannot_be_the_declared_type_exactly_is_not_pruned() {
		// Each of these must read every partition, otherwise a lossy or wrong-type hash drops matching rows.
		let int4 = [key("$1", ValueType::Int4)];
		let utf8 = [key("$1", ValueType::Utf8)];

		assert_eq!(chosen(&int4, vec![("1", Value::Int8(5_000_000_000))]), ScanPartitions::All);
		assert_eq!(chosen(&int4, vec![("1", Value::Utf8("3".to_string()))]), ScanPartitions::All);
		assert_eq!(chosen(&int4, vec![("1", Value::float8(3.0))]), ScanPartitions::All);
		assert_eq!(chosen(&int4, vec![("1", Value::none())]), ScanPartitions::All);
		assert_eq!(chosen(&int4, vec![]), ScanPartitions::All);
		assert_eq!(chosen(&utf8, vec![("1", Value::Int4(3))]), ScanPartitions::All);
	}

	#[test]
	fn same_type_keys_and_constants_hash_together_in_column_order() {
		// The partition is one hash over every key in declared order, so a constant must keep its slot.
		let keys = [PartitionKey::Constant(Value::Utf8("eu".to_string())), key("$tier", ValueType::Utf8)];

		assert_eq!(
			chosen(&keys, vec![("tier", Value::Utf8("gold".to_string()))]),
			ScanPartitions::One(Partition::of(&[
				Value::Utf8("eu".to_string()),
				Value::Utf8("gold".to_string())
			]))
		);
	}

	#[test]
	fn a_column_layout_never_prunes() {
		// A column layout refuses partitioned row-store reads, so the scan there must stay unpruned.
		let keys = [key("$1", ValueType::Int4)];
		let admin = create_test_admin_transaction();
		let mut query = QueryTransaction::new(
			admin.multi.begin_query(admin.multi.generate_transaction_id()).unwrap(),
			admin.single.clone(),
			Arc::new(TransactionContext::system(admin.context().runtime.clone())),
		);
		query.layout = ScanLayout::Column;

		assert_eq!(
			choose_partition(
				&keys,
				&Catalog::testing(),
				&mut Transaction::Query(&mut query),
				&symbols(vec![("1", Value::Int4(3))])
			)
			.unwrap(),
			ScanPartitions::All
		);
	}

	#[test]
	fn a_stored_dictionary_key_hashes_as_its_id_like_the_writer() {
		// The writer hashes the interned id, so hashing the text would name a partition no row is in.
		let (catalog, mut admin, dictionary) = dictionary_fixture();
		let id = admin.intern_values(&dictionary, vec![utf8("pool-a")]).unwrap()[0];
		let constant = [through(&dictionary, PartitionKey::Constant(utf8("pool-a")))];
		let variable = [through(&dictionary, key("$pool", ValueType::Utf8))];
		let symbols = symbols(vec![("pool", utf8("pool-a"))]);
		let mut txn = Transaction::Admin(&mut admin);

		let expected = ScanPartitions::One(Partition::of(&[Value::DictionaryId(id)]));
		assert_eq!(choose_partition(&constant, &catalog, &mut txn, &symbols).unwrap(), expected);
		assert_eq!(choose_partition(&variable, &catalog, &mut txn, &symbols).unwrap(), expected);
		assert_ne!(expected, ScanPartitions::One(Partition::of(&[utf8("pool-a")])));
	}

	#[test]
	fn a_dictionary_key_never_stored_scans_nothing_not_everything() {
		// No row can carry a value that was never interned, so a miss must prune to nothing; falling back
		// to every partition is the full scan this pruning exists to avoid.
		let (catalog, mut admin, dictionary) = dictionary_fixture();
		admin.intern_values(&dictionary, vec![utf8("pool-a")]).unwrap();
		let miss = [through(&dictionary, key("$pool", ValueType::Utf8))];
		let with_other = [through(&dictionary, key("$pool", ValueType::Utf8)), key("$tier", ValueType::Int4)];
		let symbols = symbols(vec![("pool", utf8("never")), ("tier", Value::Int8(5_000_000_000))]);
		let mut txn = Transaction::Admin(&mut admin);

		assert_eq!(choose_partition(&miss, &catalog, &mut txn, &symbols).unwrap(), ScanPartitions::Nothing);
		assert_eq!(
			choose_partition(&with_other, &catalog, &mut txn, &symbols).unwrap(),
			ScanPartitions::Nothing
		);
	}

	#[test]
	fn an_unresolved_dictionary_key_reads_every_partition() {
		// A key with no usable value says nothing about the rows, so it must not prune at all.
		let (catalog, mut admin, dictionary) = dictionary_fixture();
		let keys = [through(&dictionary, key("$pool", ValueType::Utf8))];
		let mut txn = Transaction::Admin(&mut admin);

		assert_eq!(choose_partition(&keys, &catalog, &mut txn, &symbols(vec![])).unwrap(), ScanPartitions::All);
		assert_eq!(
			choose_partition(&keys, &catalog, &mut txn, &symbols(vec![("pool", Value::Int4(3))])).unwrap(),
			ScanPartitions::All
		);
	}

	#[test]
	fn a_dictionary_in_list_keeps_its_hits_and_drops_its_misses() {
		// Each hit must be read and a miss adds no partition; an all-miss list must scan nothing, because an
		// empty partition list is not the same thing to every scan.
		let (catalog, mut admin, dictionary) = dictionary_fixture();
		let ids = admin.intern_values(&dictionary, vec![utf8("pool-a"), utf8("pool-b")]).unwrap();
		let some = [through(
			&dictionary,
			PartitionKey::In(vec![
				PartitionKey::Constant(utf8("pool-a")),
				PartitionKey::Constant(utf8("never")),
				PartitionKey::Constant(utf8("pool-b")),
			]),
		)];
		let none_found = [through(&dictionary, PartitionKey::In(vec![PartitionKey::Constant(utf8("never"))]))];
		let mut txn = Transaction::Admin(&mut admin);

		let mut expected: Vec<Partition> =
			ids.iter().map(|id| Partition::of(&[Value::DictionaryId(id.clone())])).collect();
		expected.sort_unstable_by(|a, b| b.cmp(a));
		assert_eq!(
			choose_partitions(&some, &catalog, &mut txn, &symbols(vec![])).unwrap(),
			Some(ScanPartitions::Many(expected))
		);
		assert_eq!(
			choose_partitions(&none_found, &catalog, &mut txn, &symbols(vec![])).unwrap(),
			Some(ScanPartitions::Nothing)
		);
	}

	#[test]
	fn a_dictionary_hash_collision_fails_instead_of_pruning() {
		// A slot holding other bytes is corruption; answering it as all or nothing would hide it.
		let (catalog, mut admin, dictionary) = dictionary_fixture();
		let forged = utf8("forged");
		let hash = xxh3_128(&to_stdvec(&forged).unwrap()).0.to_be_bytes();
		let mut poisoned = Vec::with_capacity(16 + 5);
		poisoned.extend_from_slice(&7u128.to_be_bytes());
		poisoned.extend_from_slice(b"other");
		let ctx = Arc::new(TransactionContext::system(admin.context().runtime.clone()));
		let mut write =
			admin.single.begin_command(&ctx, [&DictionaryEntryKey::encoded(dictionary.id, hash)]).unwrap();
		write.set(vec![(DictionaryEntryKey::new(dictionary.id, hash), EncodedPodRow::new(&poisoned))]).unwrap();
		write.commit().unwrap();
		drop(write);
		let keys = [through(&dictionary, PartitionKey::Constant(forged))];

		let err = choose_partition(&keys, &catalog, &mut Transaction::Admin(&mut admin), &symbols(vec![]))
			.unwrap_err();
		assert!(err.to_string().to_lowercase().contains("collision"), "got: {err}");
	}
}
