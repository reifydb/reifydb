// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::HashMap,
	sync::{Arc, LazyLock},
};

use reifydb_catalog::catalog::Catalog;
use reifydb_core::{
	interface::catalog::{id::NamespaceId, object::ObjectId},
	value::column::columns::Columns,
};
use reifydb_routine_abi::{Routine, RoutineInfo, context::ProcedureContext, error::RoutineError};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{
	fragment::Fragment,
	params::Params,
	value::{Value, datetime::DateTime, frame::frame::Frame, value_type::ValueType},
};

static INFO: LazyLock<RoutineInfo> = LazyLock::new(|| RoutineInfo::new("system::source::complete_through"));

const NAME: &str = "system::source::complete_through";

const COMPLETENESS: &str = "system::source::completeness";

pub struct CompleteThroughProcedure;

impl Default for CompleteThroughProcedure {
	fn default() -> Self {
		Self::new()
	}
}

impl CompleteThroughProcedure {
	pub fn new() -> Self {
		Self
	}
}

impl<'a, 'tx> Routine<ProcedureContext<'a, 'tx>> for CompleteThroughProcedure {
	fn info(&self) -> &RoutineInfo {
		&INFO
	}

	fn return_type(&self, _input_types: &[ValueType]) -> ValueType {
		ValueType::Any
	}

	fn execute(&self, ctx: &mut ProcedureContext<'a, 'tx>, _args: &Columns) -> Result<Columns, RoutineError> {
		let (objects, at) = match ctx.params {
			Params::Positional(args) if args.len() == 2 => (args[0].clone(), args[1].clone()),
			Params::Positional(args) => return Err(arity(args.len())),
			_ => return Err(arity(0)),
		};

		if !ctx.identity.is_privileged() {
			return Err(failed(
				"a completeness assertion advances every window reading the object and cannot be \
				 walked back, so it requires a privileged identity",
			));
		}

		let at = match at {
			Value::DateTime(at) => at,
			other => {
				return Err(RoutineError::ProcedureInvalidArgumentType {
					procedure: Fragment::internal(NAME),
					argument_index: 1,
					expected: vec![ValueType::DateTime],
					actual: other.get_type(),
				});
			}
		};

		let names = identifiers(&objects)?;
		let resolved = resolve_all(ctx.catalog, ctx.tx, &names)?;

		let mut asserted = Vec::with_capacity(resolved.len());
		for (name, object) in resolved {
			assert_one(ctx, object, at)?;
			asserted.push((name, object));
		}

		Ok(output(&asserted, at))
	}
}

fn arity(actual: usize) -> RoutineError {
	RoutineError::ProcedureArityMismatch {
		procedure: Fragment::internal(NAME),
		expected: 2,
		actual,
	}
}

fn failed(reason: &str) -> RoutineError {
	RoutineError::ProcedureExecutionFailed {
		procedure: Fragment::internal(NAME),
		reason: reason.to_string(),
	}
}

fn identifiers(objects: &Value) -> Result<Vec<String>, RoutineError> {
	let mut names = Vec::new();
	match objects.unwrap_any() {
		Value::Utf8(name) => names.push(name.clone()),
		Value::List(items) => {
			for item in items {
				match item.unwrap_any() {
					Value::Utf8(name) => names.push(name.clone()),
					other => {
						return Err(RoutineError::ProcedureInvalidArgumentType {
							procedure: Fragment::internal(NAME),
							argument_index: 0,
							expected: vec![ValueType::Utf8],
							actual: other.get_type(),
						});
					}
				}
			}
		}
		other => {
			return Err(RoutineError::ProcedureInvalidArgumentType {
				procedure: Fragment::internal(NAME),
				argument_index: 0,
				expected: vec![ValueType::Utf8],
				actual: other.get_type(),
			});
		}
	}

	if names.is_empty() {
		return Err(failed("no object named; an empty assertion advances nothing and hides the typo"));
	}
	Ok(names)
}

fn resolve_all(
	catalog: &Catalog,
	tx: &mut Transaction<'_>,
	names: &[String],
) -> Result<Vec<(String, ObjectId)>, RoutineError> {
	let mut resolved: Vec<(String, ObjectId)> = Vec::new();
	let mut seen = Vec::new();

	for name in names {
		let found = resolve_one(catalog, tx, name)?;
		if found.is_empty() {
			return Err(failed(&format!(
				"'{name}' names neither an object nor a namespace; a partially applied assertion \
				 advances some sources and leaves the rest pinned"
			)));
		}
		for (qualified, object) in found {
			if seen.contains(&object) {
				continue;
			}
			seen.push(object);
			resolved.push((qualified, object));
		}
	}

	Ok(resolved)
}

fn resolve_one(
	catalog: &Catalog,
	tx: &mut Transaction<'_>,
	name: &str,
) -> Result<Vec<(String, ObjectId)>, RoutineError> {
	if let Some((namespace_path, object_name)) = name.rsplit_once("::")
		&& let Some(namespace) = catalog.find_namespace_by_path(&mut tx.reborrow(), namespace_path)?
		&& let Some(object) = find_object(catalog, tx, namespace.id(), object_name)?
	{
		return Ok(vec![(name.to_string(), object)]);
	}

	let Some(namespace) = catalog.find_namespace_by_path(&mut tx.reborrow(), name)? else {
		return Ok(vec![]);
	};
	expand_namespace(catalog, tx, namespace.id(), name)
}

fn find_object(
	catalog: &Catalog,
	tx: &mut Transaction<'_>,
	namespace: NamespaceId,
	name: &str,
) -> Result<Option<ObjectId>, RoutineError> {
	if let Some(table) = catalog.find_table_by_name(&mut tx.reborrow(), namespace, name)? {
		return Ok(Some(ObjectId::Table(table.id)));
	}
	if let Some(view) = catalog.find_view_by_name(&mut tx.reborrow(), namespace, name)? {
		return Ok(Some(ObjectId::View(view.id())));
	}
	if let Some(ringbuffer) = catalog.find_ringbuffer_by_name(&mut tx.reborrow(), namespace, name)? {
		return Ok(Some(ObjectId::RingBuffer(ringbuffer.id)));
	}
	if let Some(series) = catalog.find_series_by_name(&mut tx.reborrow(), namespace, name)? {
		return Ok(Some(ObjectId::Series(series.id)));
	}
	Ok(None)
}

fn expand_namespace(
	catalog: &Catalog,
	tx: &mut Transaction<'_>,
	namespace: NamespaceId,
	path: &str,
) -> Result<Vec<(String, ObjectId)>, RoutineError> {
	let mut objects = Vec::new();

	for table in catalog.list_tables_all(&mut tx.reborrow())? {
		if table.namespace == namespace {
			objects.push((format!("{path}::{}", table.name), ObjectId::Table(table.id)));
		}
	}
	for view in catalog.list_views_all(&mut tx.reborrow())? {
		if view.namespace() == namespace {
			objects.push((format!("{path}::{}", view.name()), ObjectId::View(view.id())));
		}
	}
	for ringbuffer in catalog.list_ringbuffers_all(&mut tx.reborrow())? {
		if ringbuffer.namespace == namespace {
			objects.push((format!("{path}::{}", ringbuffer.name), ObjectId::RingBuffer(ringbuffer.id)));
		}
	}
	for series in catalog.list_series_all(&mut tx.reborrow())? {
		if series.namespace == namespace {
			objects.push((format!("{path}::{}", series.name), ObjectId::Series(series.id)));
		}
	}

	Ok(objects)
}

fn assert_one(ctx: &mut ProcedureContext<'_, '_>, object: ObjectId, at: DateTime) -> Result<(), RoutineError> {
	let key = object.to_u64();
	let params = named(key, at);

	let advanced =
		ctx.tx.rql(
			&format!("update {COMPLETENESS} {{ complete_through: $complete_through }} \
				 filter {{ object_id == $object_id and complete_through <= $complete_through }}"),
			params.clone(),
		)
		.check()
		.map_err(RoutineError::from)?;

	match rows_updated(&advanced.frames) {
		0 => {}
		1 => return Ok(()),
		updated => unreachable!(
			"{COMPLETENESS} advanced {updated} rows for object {key}; one row per object is an invariant"
		),
	}

	let current =
		ctx.tx.rql(&format!("from {COMPLETENESS} filter {{ object_id == $object_id }}"), params.clone())
			.check()
			.map_err(RoutineError::from)?;

	if let Some(previous) = single_complete_through(&current.frames, key) {
		return Err(failed(&format!(
			"object {key} is already asserted complete through {previous}; {at} is a \
			 regression and a monotone watermark would swallow it without a signal"
		)));
	}

	ctx.tx.rql(
		&format!("insert {COMPLETENESS} [{{ object_id: $object_id, \
				 complete_through: $complete_through }}]"),
		params,
	)
	.check()
	.map_err(RoutineError::from)?;
	Ok(())
}

fn rows_updated(frames: &[Frame]) -> u64 {
	frames.first()
		.and_then(|frame| frame.columns.iter().find(|c| c.name.as_str() == "updated"))
		.filter(|column| column.data.len() == 1)
		.map_or(0, |column| match column.data.get_value(0) {
			Value::Uint8(count) => count,
			_ => 0,
		})
}

fn named(object_id: u64, at: DateTime) -> Params {
	let mut map = HashMap::with_capacity(2);
	map.insert("object_id".to_string(), Value::Uint8(object_id));
	map.insert("complete_through".to_string(), Value::DateTime(at));
	Params::Named(Arc::new(map))
}

fn single_complete_through(frames: &[Frame], key: u64) -> Option<DateTime> {
	let frame = frames.first()?;
	let column = frame.columns.iter().find(|c| c.name.as_str() == "complete_through")?;
	match column.data.len() {
		0 => return None,
		1 => {}
		recorded => unreachable!(
			"{COMPLETENESS} holds {recorded} rows for object {key}; one row per object is an invariant"
		),
	}
	match column.data.get_value(0) {
		Value::DateTime(at) => Some(at),
		_ => None,
	}
}

fn output(asserted: &[(String, ObjectId)], at: DateTime) -> Columns {
	let names = ["object", "object_id", "complete_through"];
	let rows: Vec<Vec<Value>> = asserted
		.iter()
		.map(|(name, object)| {
			vec![Value::Utf8(name.clone()), Value::Uint8(object.to_u64()), Value::DateTime(at)]
		})
		.collect();
	Columns::from_rows(&names, &rows)
}
