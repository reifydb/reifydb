// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::str::FromStr;

use reifydb_core::{
	common::TimeSource,
	error::diagnostic::query::column_not_found,
	expression::{Expression, name::display_label},
	interface::resolved::{
		ResolvedObject, ResolvedQueue, ResolvedRingBuffer, ResolvedSeries, ResolvedTable, ResolvedView,
	},
	sort::SortKey,
};
use reifydb_value::{
	Result,
	error::Error,
	fragment::Fragment,
	value::{system_columns::SystemColumn, value_type::ValueType},
};

use crate::{
	bump::BumpBox,
	nodes::InlineDataNode,
	plan::physical::{AppendPhysicalNode, AppendPhysicalSource, AssignValue, LetValue, PhysicalPlan, ReturnValue},
};

pub fn named_system_columns(plan: &PhysicalPlan<'_>) -> Result<Vec<SystemColumn>> {
	statement(plan, Scope::new(false))
}

pub fn check_system_columns(plan: &PhysicalPlan<'_>) -> Result<()> {
	statement(plan, Scope::new(false)).map(drop)
}

pub fn check_flow_system_columns(plan: &PhysicalPlan<'_>) -> Result<()> {
	statement(plan, Scope::new(true)).map(drop)
}

#[derive(Debug, Clone)]
struct Reference {
	column: SystemColumn,
	fragment: Fragment,
}

impl Reference {
	fn same(&self, other: &Reference) -> bool {
		self.column == other.column && self.fragment == other.fragment
	}
}

#[derive(Debug, Clone)]
struct Unmet {
	reference: Reference,
	note: String,
}

#[derive(Debug, Default)]
struct Found {
	named: Vec<SystemColumn>,
	unmet: Vec<Unmet>,
}

impl Found {
	fn named(named: Vec<SystemColumn>) -> Self {
		Self {
			named,
			unmet: Vec::new(),
		}
	}

	fn merge(mut self, other: Found) -> Self {
		self.named.extend(other.named);
		self.unmet.extend(other.unmet);
		self
	}
}

#[derive(Clone)]
struct Scope {
	references: Vec<Reference>,
	flow: bool,
}

impl Scope {
	fn new(flow: bool) -> Self {
		Self {
			references: Vec::new(),
			flow,
		}
	}

	fn fresh(&self) -> Self {
		Self::new(self.flow)
	}

	fn in_flow(&self) -> Self {
		Self::new(true)
	}

	fn with(mut self, references: Vec<Reference>) -> Self {
		self.references.extend(references);
		self
	}

	fn supplied_by(mut self, expressions: &[Expression]) -> Self {
		let labels: Vec<Option<SystemColumn>> =
			expressions.iter().map(|expression| system_column(display_label(expression).text())).collect();
		self.references.retain(|reference| !labels.contains(&Some(reference.column)));
		self
	}

	fn columns(&self) -> Vec<SystemColumn> {
		self.references.iter().map(|reference| reference.column).collect()
	}
}

fn system_column(name: &str) -> Option<SystemColumn> {
	let bare = name.strip_prefix('#').unwrap_or(name);
	SystemColumn::ALL.into_iter().find(|column| &column.name()[1..] == bare)
}

fn statement(plan: &PhysicalPlan<'_>, scope: Scope) -> Result<Vec<SystemColumn>> {
	let Found {
		named,
		unmet,
	} = visit(plan, scope)?;
	match unmet.into_iter().next() {
		None => Ok(SystemColumn::ALL.into_iter().filter(|column| named.contains(column)).collect()),
		Some(Unmet {
			reference,
			note,
		}) => {
			let mut diagnostic = column_not_found(reference.fragment);
			diagnostic.notes.push(note);
			Err(Error(Box::new(diagnostic)))
		}
	}
}

fn statements(plans: &[PhysicalPlan<'_>], scope: &Scope) -> Result<Found> {
	let mut named = Vec::new();
	for plan in plans {
		named.extend(statement(plan, scope.fresh())?);
	}
	Ok(Found::named(named))
}

fn nested(plan: &PhysicalPlan<'_>, scope: &Scope) -> Result<Found> {
	Ok(Found::named(statement(plan, scope.fresh())?))
}

fn visit(plan: &PhysicalPlan<'_>, scope: Scope) -> Result<Found> {
	match plan {
		PhysicalPlan::TableScan(node) => Ok(resolve(Source::Table(&node.source), &scope)),
		PhysicalPlan::IndexScan(node) => Ok(resolve(Source::Table(&node.source), &scope)),
		PhysicalPlan::ViewScan(node) => Ok(resolve(Source::View(&node.source), &scope)),
		PhysicalPlan::RingBufferScan(node) => Ok(resolve(Source::RingBuffer(&node.source), &scope)),
		PhysicalPlan::SeriesScan(node) => Ok(resolve(Source::Series(&node.source), &scope)),
		PhysicalPlan::QueueScan(node) => Ok(resolve(Source::Queue(&node.source), &scope)),
		PhysicalPlan::DictionaryScan(_) => Ok(resolve(Source::Dictionary, &scope)),
		PhysicalPlan::InlineData(node) => Ok(resolve(Source::InlineData(node), &scope)),
		PhysicalPlan::RowPointLookup(node) => Ok(lookup(&node.source, &scope)),
		PhysicalPlan::RowListLookup(node) => Ok(lookup(&node.source, &scope)),
		PhysicalPlan::RowRangeScan(node) => Ok(lookup(&node.source, &scope)),
		PhysicalPlan::CallFunction(_)
		| PhysicalPlan::Variable(_)
		| PhysicalPlan::RemoteScan(_)
		| PhysicalPlan::TableVirtualScan(_)
		| PhysicalPlan::Generator(_)
		| PhysicalPlan::Environment(_) => Ok(Found::named(scope.columns())),
		PhysicalPlan::Filter(node) => visit(&node.input, scope.with(references(&node.conditions))),
		PhysicalPlan::Gate(node) => visit(&node.input, scope.with(references(&node.conditions))),
		PhysicalPlan::Sort(node) => visit(&node.input, scope.with(sort_references(&node.by))),
		PhysicalPlan::Take(node) => visit(&node.input, scope),
		PhysicalPlan::Distinct(node) => {
			let distinct = node
				.columns
				.iter()
				.filter_map(|column| {
					system_column(column.name()).map(|system| Reference {
						column: system,
						fragment: column.identifier().clone(),
					})
				})
				.collect();
			visit(&node.input, scope.with(distinct))
		}
		PhysicalPlan::Assert(node) => optional(&node.input, scope.with(references(&node.conditions))),
		PhysicalPlan::Map(node) => {
			optional(&node.input, scope.supplied_by(&node.map).with(references(&node.map)))
		}
		PhysicalPlan::Extend(node) => {
			optional(&node.input, scope.supplied_by(&node.extend).with(references(&node.extend)))
		}
		PhysicalPlan::Patch(node) => {
			optional(&node.input, scope.supplied_by(&node.assignments).with(references(&node.assignments)))
		}
		PhysicalPlan::Apply(node) => optional(&node.input, scope.with(references(&node.params))),
		PhysicalPlan::Aggregate(node) => {
			visit(&node.input, scope.fresh().with(references(node.by.iter().chain(node.map.iter()))))
		}
		PhysicalPlan::Window(node) => optional(
			&node.input,
			scope.fresh().with(references(node.group_by.iter().chain(node.aggregations.iter()))),
		),
		PhysicalPlan::Scalarize(node) => visit(&node.input, scope.fresh()),
		PhysicalPlan::JoinInner(node) => join(&node.left, &node.right, &node.on, scope),
		PhysicalPlan::JoinLeft(node) => join(&node.left, &node.right, &node.on, scope),
		PhysicalPlan::JoinNatural(node) => join(&node.left, &node.right, &[], scope),
		PhysicalPlan::Lookup(node) => join(&node.left, &node.right, &node.on, scope),
		PhysicalPlan::Append(AppendPhysicalNode::Query {
			left,
			right,
			..
		}) => Ok(visit(left, scope.clone())?.merge(visit(right, scope)?)),
		PhysicalPlan::Append(AppendPhysicalNode::IntoVariable {
			source: AppendPhysicalSource::Statement(plans),
			..
		}) => statements(plans, &scope),
		PhysicalPlan::InsertTable(node) => {
			let found = visit(&node.input, scope.fresh())?;
			returning(found, Source::Table(&node.target), &node.returning, &scope)
		}
		PhysicalPlan::InsertRingBuffer(node) => {
			let found = visit(&node.input, scope.fresh())?;
			returning(found, Source::RingBuffer(&node.target), &node.returning, &scope)
		}
		PhysicalPlan::InsertSeries(node) => {
			let found = visit(&node.input, scope.fresh())?;
			returning(found, Source::Series(&node.target), &node.returning, &scope)
		}
		PhysicalPlan::InsertQueue(node) => {
			let own = references(node.deduplication_key.iter().chain(node.not_before.iter()));
			let found = visit(&node.input, scope.fresh().with(own))?;
			returning(found, Source::Queue(&node.target), &node.returning, &scope)
		}
		PhysicalPlan::InsertDictionary(node) => {
			let found = visit(&node.input, scope.fresh())?;
			returning(found, Source::Dictionary, &node.returning, &scope)
		}
		PhysicalPlan::Update(node) => {
			let found = visit(&node.input, scope.fresh())?;
			match &node.target {
				Some(target) => returning(found, Source::Table(target), &node.returning, &scope),
				None => Ok(found),
			}
		}
		PhysicalPlan::UpdateRingBuffer(node) => {
			let found = visit(&node.input, scope.fresh())?;
			returning(found, Source::RingBuffer(&node.target), &node.returning, &scope)
		}
		PhysicalPlan::UpdateSeries(node) => {
			let found = visit(&node.input, scope.fresh())?;
			returning(found, Source::Series(&node.target), &node.returning, &scope)
		}
		PhysicalPlan::Delete(node) => {
			let found = optional(&node.input, scope.fresh())?;
			match &node.target {
				Some(target) => returning(found, Source::Table(target), &node.returning, &scope),
				None => Ok(found),
			}
		}
		PhysicalPlan::DeleteRingBuffer(node) => {
			let found = optional(&node.input, scope.fresh())?;
			returning(found, Source::RingBuffer(&node.target), &node.returning, &scope)
		}
		PhysicalPlan::DeleteSeries(node) => {
			let found = optional(&node.input, scope.fresh())?;
			returning(found, Source::Series(&node.target), &node.returning, &scope)
		}
		PhysicalPlan::Declare(node) => match &node.value {
			LetValue::Statement(plan) => nested(plan, &scope),
			_ => Ok(Found::default()),
		},
		PhysicalPlan::Assign(node) => match &node.value {
			AssignValue::Statement(plan) => nested(plan, &scope),
			_ => Ok(Found::default()),
		},
		PhysicalPlan::Return(node) => match &node.value {
			Some(ReturnValue::Statement(plan)) => nested(plan, &scope),
			_ => Ok(Found::default()),
		},
		PhysicalPlan::Conditional(node) => {
			let mut found = nested(&node.then_branch, &scope)?;
			for branch in &node.else_ifs {
				found = found.merge(nested(&branch.then_branch, &scope)?);
			}
			if let Some(branch) = &node.else_branch {
				found = found.merge(nested(branch, &scope)?);
			}
			Ok(found)
		}
		PhysicalPlan::Loop(node) => statements(&node.body, &scope),
		PhysicalPlan::While(node) => statements(&node.body, &scope),
		PhysicalPlan::For(node) => Ok(nested(&node.iterable, &scope)?.merge(statements(&node.body, &scope)?)),
		PhysicalPlan::DefineFunction(node) => statements(&node.body, &scope),
		PhysicalPlan::DefineClosure(node) => statements(&node.body, &scope),
		PhysicalPlan::CreateDeferredView(node) => {
			statement(&node.as_clause, scope.in_flow())?;
			Ok(Found::default())
		}
		PhysicalPlan::CreateTransactionalView(node) => {
			statement(&node.as_clause, scope.in_flow())?;
			Ok(Found::default())
		}
		_ => Ok(Found::default()),
	}
}

fn optional(input: &Option<BumpBox<'_, PhysicalPlan<'_>>>, scope: Scope) -> Result<Found> {
	match input {
		Some(input) => visit(input, scope),
		None => Ok(Found::default()),
	}
}

fn join(left: &PhysicalPlan<'_>, right: &PhysicalPlan<'_>, on: &[Expression], scope: Scope) -> Result<Found> {
	let on = references(on);
	let is_on = |unmet: &Unmet| on.iter().any(|reference| reference.same(&unmet.reference));
	let right_scope = scope.fresh().with(on.clone());
	let left = visit(left, scope.with(on.clone()))?;
	let right = visit(right, right_scope)?;
	let lacked_by_right = |unmet: &Unmet| right.unmet.iter().any(|other| other.reference.same(&unmet.reference));
	let mut unmet: Vec<Unmet> =
		left.unmet.into_iter().filter(|unmet| !is_on(unmet) || lacked_by_right(unmet)).collect();
	unmet.extend(right.unmet.iter().filter(|unmet| !is_on(unmet)).cloned());
	Ok(Found {
		named: left.named.into_iter().chain(right.named).collect(),
		unmet,
	})
}

fn returning(found: Found, target: Source<'_>, expressions: &Option<Vec<Expression>>, scope: &Scope) -> Result<Found> {
	match expressions {
		Some(expressions) => Ok(found.merge(resolve(target, &scope.fresh().with(references(expressions))))),
		None => Ok(found),
	}
}

fn lookup(source: &ResolvedObject, scope: &Scope) -> Found {
	match source {
		ResolvedObject::Table(table) => resolve(Source::Table(table), scope),
		ResolvedObject::View(view) => resolve(Source::View(view), scope),
		ResolvedObject::RingBuffer(ringbuffer) => resolve(Source::RingBuffer(ringbuffer), scope),
		_ => Found::named(scope.columns()),
	}
}

enum Source<'a> {
	Table(&'a ResolvedTable),
	View(&'a ResolvedView),
	RingBuffer(&'a ResolvedRingBuffer),
	Series(&'a ResolvedSeries),
	Queue(&'a ResolvedQueue),
	Dictionary,
	InlineData(&'a InlineDataNode),
}

impl Source<'_> {
	fn has_user_column(&self, name: &str) -> bool {
		match self {
			Source::Table(table) => table.columns().iter().any(|column| column.name == name),
			Source::View(view) => view.columns().iter().any(|column| column.name == name),
			Source::RingBuffer(ringbuffer) => ringbuffer.columns().iter().any(|column| column.name == name),
			Source::Series(series) => series.columns().iter().any(|column| column.name == name),
			Source::Queue(queue) => queue.columns().iter().any(|column| column.name == name),
			Source::Dictionary => name == "id" || name == "value",
			Source::InlineData(node) => {
				node.rows.iter().flatten().any(|alias| alias.alias.0.text() == name)
			}
		}
	}

	fn lacks(&self, column: SystemColumn, flow: bool) -> Option<String> {
		if flow && matches!(column, SystemColumn::Partitions | SystemColumn::CommitVersion) {
			return Some(format!("flow inputs have no {column}"));
		}
		match self {
			Source::Table(table) => {
				let def = table.def();
				object_lacks(
					"table",
					table.namespace().name(),
					table.name(),
					column,
					def.partition_by.is_empty(),
					&def.time,
				)
			}
			Source::View(_) => matches!(column, SystemColumn::Partitions | SystemColumn::CommitVersion)
				.then(|| format!("views have no {column}")),
			Source::RingBuffer(ringbuffer) => {
				if column == SystemColumn::CommitVersion {
					return Some(format!("ringbuffers have no {column}"));
				}
				let def = ringbuffer.def();
				object_lacks(
					"ringbuffer",
					ringbuffer.namespace().name(),
					ringbuffer.name(),
					column,
					def.partition_by.is_empty(),
					&def.time,
				)
			}
			Source::Series(series) => {
				let def = series.def();
				object_lacks(
					"series",
					series.namespace().name(),
					series.name(),
					column,
					def.partition_by.is_empty(),
					&def.time,
				)
			}
			Source::Queue(queue) => {
				if matches!(column, SystemColumn::Partitions | SystemColumn::CommitVersion) {
					return Some(format!("queues have no {column}"));
				}
				object_lacks(
					"queue",
					queue.namespace().name(),
					queue.name(),
					column,
					false,
					&queue.def().time,
				)
			}
			Source::Dictionary => Some(format!("dictionaries have no {column}")),
			Source::InlineData(_) => Some(format!("inline data has no {column}")),
		}
	}
}

fn object_lacks(
	kind: &str,
	namespace: &str,
	name: &str,
	column: SystemColumn,
	unpartitioned: bool,
	time: &TimeSource,
) -> Option<String> {
	match column {
		SystemColumn::Partitions if unpartitioned => {
			Some(format!("{kind} {namespace}::{name} is not partitioned, so it has no {column}"))
		}
		SystemColumn::Time if *time == TimeSource::None => {
			Some(format!("{kind} {namespace}::{name} has no time source, so it has no {column}"))
		}
		_ => None,
	}
}

fn resolve(source: Source<'_>, scope: &Scope) -> Found {
	let mut found = Found::default();
	for reference in &scope.references {
		if source.has_user_column(&reference.column.name()[1..]) {
			continue;
		}
		match source.lacks(reference.column, scope.flow) {
			Some(note) => found.unmet.push(Unmet {
				reference: reference.clone(),
				note,
			}),
			None => found.named.push(reference.column),
		}
	}
	found
}

fn sort_references(keys: &[SortKey]) -> Vec<Reference> {
	keys.iter()
		.filter_map(|key| {
			system_column(key.column.text()).map(|column| Reference {
				column,
				fragment: key.column.clone(),
			})
		})
		.collect()
}

fn references<'a>(expressions: impl IntoIterator<Item = &'a Expression>) -> Vec<Reference> {
	let mut found = Vec::new();
	for expression in expressions {
		collect(expression, &mut found);
	}
	found
}

fn push(name: &Fragment, found: &mut Vec<Reference>) {
	if let Some(column) = system_column(name.text()) {
		found.push(Reference {
			column,
			fragment: name.clone(),
		});
	}
}

fn collect(expression: &Expression, found: &mut Vec<Reference>) {
	match expression {
		Expression::Column(column) => push(&column.0.name, found),
		Expression::AccessSource(access) => push(&access.column.name, found),
		Expression::Alias(alias) => collect(&alias.expression, found),
		Expression::Call(call) => {
			call.args.iter().filter(|arg| !names_a_type(arg)).for_each(|arg| collect(arg, found))
		}
		Expression::Cast(cast) => collect(&cast.expression, found),
		Expression::Prefix(prefix) => collect(&prefix.expression, found),
		Expression::Add(e) => both(&e.left, &e.right, found),
		Expression::Sub(e) => both(&e.left, &e.right, found),
		Expression::Mul(e) => both(&e.left, &e.right, found),
		Expression::Div(e) => both(&e.left, &e.right, found),
		Expression::Rem(e) => both(&e.left, &e.right, found),
		Expression::GreaterThan(e) => both(&e.left, &e.right, found),
		Expression::GreaterThanEqual(e) => both(&e.left, &e.right, found),
		Expression::LessThan(e) => both(&e.left, &e.right, found),
		Expression::LessThanEqual(e) => both(&e.left, &e.right, found),
		Expression::Equal(e) => both(&e.left, &e.right, found),
		Expression::NotEqual(e) => both(&e.left, &e.right, found),
		Expression::And(e) => both(&e.left, &e.right, found),
		Expression::Or(e) => both(&e.left, &e.right, found),
		Expression::Xor(e) => both(&e.left, &e.right, found),
		Expression::Between(between) => {
			collect(&between.value, found);
			both(&between.lower, &between.upper, found);
		}
		Expression::In(e) => both(&e.value, &e.list, found),
		Expression::Contains(e) => both(&e.value, &e.list, found),
		Expression::If(e) => {
			both(&e.condition, &e.then_expr, found);
			for else_if in &e.else_ifs {
				both(&else_if.condition, &else_if.then_expr, found);
			}
			if let Some(else_expr) = &e.else_expr {
				collect(else_expr, found);
			}
		}
		Expression::Tuple(e) => e.expressions.iter().for_each(|inner| collect(inner, found)),
		Expression::List(e) => e.expressions.iter().for_each(|inner| collect(inner, found)),
		Expression::Map(e) => e.expressions.iter().for_each(|inner| collect(inner, found)),
		Expression::Extend(e) => e.expressions.iter().for_each(|inner| collect(inner, found)),
		Expression::SumTypeConstructor(e) => e.columns.iter().for_each(|(_, inner)| collect(inner, found)),
		Expression::IsVariant(e) => collect(&e.expression, found),
		Expression::FieldAccess(e) => collect(&e.object, found),
		Expression::Constant(_) | Expression::Type(_) | Expression::Parameter(_) | Expression::Variable(_) => {}
	}
}

fn names_a_type(expression: &Expression) -> bool {
	matches!(expression, Expression::Column(column) if ValueType::from_str(column.0.name.text()).is_ok())
}

fn both(left: &Expression, right: &Expression, found: &mut Vec<Reference>) {
	collect(left, found);
	collect(right, found);
}
