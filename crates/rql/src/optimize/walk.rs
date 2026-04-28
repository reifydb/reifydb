// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use crate::{
	expression::Expression,
	plan::physical::{AppendPhysicalNode, AppendPhysicalSource, AssignValue, LetValue, PhysicalPlan},
};

/// Visit every `Expression` contained in `plan` (recursively, including child plans).
///
/// `internal` is invoked for expressions whose label is not user-visible (filter conditions,
/// join keys, control-flow conditions, function arguments, etc.). `projection` is invoked
/// for expressions that produce a user-visible output column (Map.map, Extend.extend,
/// Aggregate.map, Aggregate.by, Window.aggregations, Window.group_by, DML returning clauses).
pub fn walk_expressions_mut(
	plan: &mut PhysicalPlan<'_>,
	internal: &mut dyn FnMut(&mut Expression),
	projection: &mut dyn FnMut(&mut Expression),
) {
	match plan {
		PhysicalPlan::Aggregate(n) => {
			for e in &mut n.by {
				projection(e);
			}
			for e in &mut n.map {
				projection(e);
			}
			walk_expressions_mut(&mut n.input, internal, projection);
		}
		PhysicalPlan::Assert(n) => {
			// Skip folding assert conditions: the runtime error reports the
			// original expression text (e.g. "this expression is false: 1 == 2").
			// Folding would replace 1 == 2 with false and lose that detail.
			if let Some(input) = n.input.as_mut() {
				walk_expressions_mut(input, internal, projection);
			}
		}
		PhysicalPlan::Filter(n) => {
			for e in &mut n.conditions {
				internal(e);
			}
			walk_expressions_mut(&mut n.input, internal, projection);
		}
		PhysicalPlan::Gate(n) => {
			for e in &mut n.conditions {
				internal(e);
			}
			walk_expressions_mut(&mut n.input, internal, projection);
		}
		PhysicalPlan::JoinInner(n) => {
			for e in &mut n.on {
				internal(e);
			}
			walk_expressions_mut(&mut n.left, internal, projection);
			walk_expressions_mut(&mut n.right, internal, projection);
		}
		PhysicalPlan::JoinLeft(n) => {
			for e in &mut n.on {
				internal(e);
			}
			walk_expressions_mut(&mut n.left, internal, projection);
			walk_expressions_mut(&mut n.right, internal, projection);
		}
		PhysicalPlan::JoinNatural(n) => {
			walk_expressions_mut(&mut n.left, internal, projection);
			walk_expressions_mut(&mut n.right, internal, projection);
		}
		PhysicalPlan::Take(n) => walk_expressions_mut(&mut n.input, internal, projection),
		PhysicalPlan::Sort(n) => walk_expressions_mut(&mut n.input, internal, projection),
		PhysicalPlan::Distinct(n) => walk_expressions_mut(&mut n.input, internal, projection),
		PhysicalPlan::Scalarize(n) => walk_expressions_mut(&mut n.input, internal, projection),
		PhysicalPlan::Map(n) => {
			for e in &mut n.map {
				projection(e);
			}
			if let Some(input) = n.input.as_mut() {
				walk_expressions_mut(input, internal, projection);
			}
		}
		PhysicalPlan::Extend(n) => {
			for e in &mut n.extend {
				projection(e);
			}
			if let Some(input) = n.input.as_mut() {
				walk_expressions_mut(input, internal, projection);
			}
		}
		PhysicalPlan::Patch(n) => {
			// Skip folding UPDATE SET assignments: saturation / coercion errors
			// reference the original source span when assigning to typed columns.
			// Folding 0 - 1 to -1 would relocate the diagnostic to a synthesized
			// fragment with no source position.
			if let Some(input) = n.input.as_mut() {
				walk_expressions_mut(input, internal, projection);
			}
		}
		PhysicalPlan::Apply(n) => {
			for e in &mut n.expressions {
				internal(e);
			}
			if let Some(input) = n.input.as_mut() {
				walk_expressions_mut(input, internal, projection);
			}
		}
		PhysicalPlan::Window(n) => {
			for e in &mut n.group_by {
				projection(e);
			}
			for e in &mut n.aggregations {
				projection(e);
			}
			if let Some(input) = n.input.as_mut() {
				walk_expressions_mut(input, internal, projection);
			}
		}
		PhysicalPlan::Conditional(n) => {
			internal(&mut n.condition);
			walk_expressions_mut(&mut n.then_branch, internal, projection);
			for elif in &mut n.else_ifs {
				internal(&mut elif.condition);
				walk_expressions_mut(&mut elif.then_branch, internal, projection);
			}
			if let Some(else_branch) = n.else_branch.as_mut() {
				walk_expressions_mut(else_branch, internal, projection);
			}
		}
		PhysicalPlan::Loop(n) => {
			for child in &mut n.body {
				walk_expressions_mut(child, internal, projection);
			}
		}
		PhysicalPlan::While(n) => {
			internal(&mut n.condition);
			for child in &mut n.body {
				walk_expressions_mut(child, internal, projection);
			}
		}
		PhysicalPlan::For(n) => {
			walk_expressions_mut(&mut n.iterable, internal, projection);
			for child in &mut n.body {
				walk_expressions_mut(child, internal, projection);
			}
		}
		PhysicalPlan::DefineFunction(n) => {
			for child in &mut n.body {
				walk_expressions_mut(child, internal, projection);
			}
		}
		PhysicalPlan::DefineClosure(n) => {
			for child in &mut n.body {
				walk_expressions_mut(child, internal, projection);
			}
		}
		PhysicalPlan::Declare(n) => match &mut n.value {
			LetValue::Expression(e) => internal(e),
			LetValue::Statement(plan) => walk_expressions_mut(plan, internal, projection),
			LetValue::EmptyFrame => {}
		},
		PhysicalPlan::Assign(n) => match &mut n.value {
			AssignValue::Expression(e) => internal(e),
			AssignValue::Statement(plan) => walk_expressions_mut(plan, internal, projection),
		},
		PhysicalPlan::Append(n) => match n {
			AppendPhysicalNode::Query {
				left,
				right,
			} => {
				walk_expressions_mut(left, internal, projection);
				walk_expressions_mut(right, internal, projection);
			}
			AppendPhysicalNode::IntoVariable {
				source,
				..
			} => match source {
				AppendPhysicalSource::Statement(plans) => {
					for child in plans {
						walk_expressions_mut(child, internal, projection);
					}
				}
				AppendPhysicalSource::Inline(_) => {}
			},
		},
		PhysicalPlan::CallFunction(n) => {
			for arg in &mut n.arguments {
				internal(arg);
			}
		}
		PhysicalPlan::Return(n) => {
			if let Some(e) = n.value.as_mut() {
				internal(e);
			}
		}
		// DML nodes: the .returning slot's source spans matter for column-coercion errors,
		// so leave those expressions alone. Recurse into the input subtree where any
		// nested Map / Filter / etc. is fair game.
		PhysicalPlan::Delete(n) => {
			if let Some(input) = n.input.as_mut() {
				walk_expressions_mut(input, internal, projection);
			}
		}
		PhysicalPlan::DeleteRingBuffer(n) => {
			if let Some(input) = n.input.as_mut() {
				walk_expressions_mut(input, internal, projection);
			}
		}
		PhysicalPlan::DeleteSeries(n) => {
			if let Some(input) = n.input.as_mut() {
				walk_expressions_mut(input, internal, projection);
			}
		}
		PhysicalPlan::InsertTable(n) => walk_expressions_mut(&mut n.input, internal, projection),
		PhysicalPlan::InsertRingBuffer(n) => walk_expressions_mut(&mut n.input, internal, projection),
		PhysicalPlan::InsertDictionary(n) => walk_expressions_mut(&mut n.input, internal, projection),
		PhysicalPlan::InsertSeries(n) => walk_expressions_mut(&mut n.input, internal, projection),
		PhysicalPlan::Update(n) => walk_expressions_mut(&mut n.input, internal, projection),
		PhysicalPlan::UpdateRingBuffer(n) => walk_expressions_mut(&mut n.input, internal, projection),
		PhysicalPlan::UpdateSeries(n) => walk_expressions_mut(&mut n.input, internal, projection),
		PhysicalPlan::CreateDeferredView(n) => walk_expressions_mut(&mut n.as_clause, internal, projection),
		PhysicalPlan::CreateTransactionalView(n) => {
			walk_expressions_mut(&mut n.as_clause, internal, projection)
		}
		PhysicalPlan::CreateSubscription(n) => {
			if let Some(as_clause) = n.as_clause.as_mut() {
				walk_expressions_mut(as_clause, internal, projection);
			}
		}
		// Variants that carry no Expression and no child PhysicalPlan worth descending into.
		PhysicalPlan::AssertBlock(_)
		| PhysicalPlan::IndexScan(_)
		| PhysicalPlan::RowPointLookup(_)
		| PhysicalPlan::RowListLookup(_)
		| PhysicalPlan::RowRangeScan(_)
		| PhysicalPlan::TableScan(_)
		| PhysicalPlan::TableVirtualScan(_)
		| PhysicalPlan::ViewScan(_)
		| PhysicalPlan::RingBufferScan(_)
		| PhysicalPlan::DictionaryScan(_)
		| PhysicalPlan::SeriesScan(_)
		| PhysicalPlan::Generator(_)
		| PhysicalPlan::InlineData(_)
		| PhysicalPlan::RemoteScan(_)
		| PhysicalPlan::Variable(_)
		| PhysicalPlan::Environment(_)
		| PhysicalPlan::Break
		| PhysicalPlan::Continue
		| PhysicalPlan::CreateNamespace(_)
		| PhysicalPlan::CreateRemoteNamespace(_)
		| PhysicalPlan::CreateTable(_)
		| PhysicalPlan::CreateRingBuffer(_)
		| PhysicalPlan::CreateDictionary(_)
		| PhysicalPlan::CreateSumType(_)
		| PhysicalPlan::CreatePrimaryKey(_)
		| PhysicalPlan::CreateColumnProperty(_)
		| PhysicalPlan::CreateProcedure(_)
		| PhysicalPlan::CreateEvent(_)
		| PhysicalPlan::CreateSeries(_)
		| PhysicalPlan::CreateTag(_)
		| PhysicalPlan::CreateSource(_)
		| PhysicalPlan::CreateSink(_)
		| PhysicalPlan::CreateBinding(_)
		| PhysicalPlan::CreateTest(_)
		| PhysicalPlan::RunTests(_)
		| PhysicalPlan::CreateMigration(_)
		| PhysicalPlan::Migrate(_)
		| PhysicalPlan::RollbackMigration(_)
		| PhysicalPlan::Dispatch(_)
		| PhysicalPlan::DropNamespace(_)
		| PhysicalPlan::DropTable(_)
		| PhysicalPlan::DropView(_)
		| PhysicalPlan::DropRingBuffer(_)
		| PhysicalPlan::DropDictionary(_)
		| PhysicalPlan::DropSumType(_)
		| PhysicalPlan::DropSubscription(_)
		| PhysicalPlan::DropSeries(_)
		| PhysicalPlan::DropSource(_)
		| PhysicalPlan::DropSink(_)
		| PhysicalPlan::DropProcedure(_)
		| PhysicalPlan::DropHandler(_)
		| PhysicalPlan::DropTest(_)
		| PhysicalPlan::DropBinding(_)
		| PhysicalPlan::AlterSequence(_)
		| PhysicalPlan::AlterTable(_)
		| PhysicalPlan::AlterRemoteNamespace(_)
		| PhysicalPlan::CreateIdentity(_)
		| PhysicalPlan::CreateRole(_)
		| PhysicalPlan::Grant(_)
		| PhysicalPlan::Revoke(_)
		| PhysicalPlan::DropIdentity(_)
		| PhysicalPlan::DropRole(_)
		| PhysicalPlan::CreateAuthentication(_)
		| PhysicalPlan::DropAuthentication(_)
		| PhysicalPlan::CreatePolicy(_)
		| PhysicalPlan::AlterPolicy(_)
		| PhysicalPlan::DropPolicy(_) => {}
	}
}
