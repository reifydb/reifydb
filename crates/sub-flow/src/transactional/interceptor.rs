// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Interceptors for transactional (inline) view processing.
//!
//! **Pre-commit** (`TransactionalFlowPreCommitInterceptor`):
//! When a `CommandTransaction` or `AdminTransaction` commits, runs any
//! transactional flows that depend on the tables changed in the transaction.
//! The view writes produced by those flows are fed back into the transaction
//! as `ctx.pending_writes` and committed atomically with the original DML.
//!
//! **Post-commit** (`TransactionalFlowPostCommitInterceptor`):
//! After a `CREATE VIEW` (transactional) commits, eagerly registers the
//! flow so it is available for the very next transaction's pre-commit phase.

use std::{
	collections::BTreeMap,
	sync::{Arc, RwLock},
};

use reifydb_catalog::catalog::Catalog;
use reifydb_core::{
	common::CommitVersion,
	interface::{
		catalog::{flow::FlowId, primitive::PrimitiveId},
		change::{Change, ChangeOrigin, Diff},
	},
};
use reifydb_engine::engine::StandardEngine;
use reifydb_transaction::{
	change::OperationType,
	interceptor::{
		interceptors::Interceptors,
		transaction::{PostCommitContext, PostCommitInterceptor, PreCommitContext, PreCommitInterceptor},
	},
	multi::transaction::read::MultiReadTransaction,
};
use reifydb_type::Result;
use tracing::warn;

use crate::{
	engine::FlowEngine,
	transaction::{
		FlowTransaction,
		pending::{Pending, PendingWrite},
	},
	transactional::registrar::TransactionalFlowRegistrar,
};

/// Pre-commit interceptor that executes transactional (inline) flows.
///
/// This interceptor holds a separate `FlowEngine` containing ONLY transactional
/// views — it is distinct from the coordinator's CDC `FlowEngine` which handles
/// deferred views.
pub struct TransactionalFlowPreCommitInterceptor {
	/// The flow engine containing only transactional view flows.
	pub flow_engine: Arc<RwLock<FlowEngine>>,
	/// The standard engine used to create read transactions for flow processing.
	pub engine: StandardEngine,
	/// The catalog for metadata access inside flow transactions.
	pub catalog: Catalog,
}

impl PreCommitInterceptor for TransactionalFlowPreCommitInterceptor {
	fn intercept(&self, ctx: &mut PreCommitContext) -> Result<()> {
		if ctx.flow_changes.is_empty() {
			return Ok(());
		}

		let engine = self.flow_engine.read().unwrap();
		let execution_order = engine.calculate_execution_order();

		if execution_order.is_empty() {
			return Ok(());
		}

		// Stamp all incoming changes with the current read version so that
		// downstream operators (including FFI round-trips) always see a
		// non-zero version.
		let read_version = {
			let q: MultiReadTransaction = self.engine.multi().begin_query()?;
			q.version()
		};

		// Merge per-row changes from the same origin into batched changes
		// (matching the deferred/CDC path which batches multiple diffs per Change).
		let mut available_changes: Vec<Change> = merge_changes_by_origin(&ctx.flow_changes, read_version);

		for flow_id in execution_order {
			// Filter changes relevant to this flow using existing source routing.
			let relevant: Vec<Change> = available_changes
				.iter()
				.filter(|c| flow_is_interested_in(c, flow_id, &engine))
				.cloned()
				.collect();

			if relevant.is_empty() {
				continue;
			}

			// Create a flow transaction with no version restriction (reads latest committed).
			let primitive_query: MultiReadTransaction = self.engine.multi().begin_query()?;
			let state_query: MultiReadTransaction = self.engine.multi().begin_query()?;
			let interceptors: Interceptors = self.engine.create_interceptors();

			// Seed base_pending from the committing transaction's writes so
			// flow operators can see uncommitted row data.
			let mut base_pending = Pending::new();
			for (key, value) in &ctx.transaction_writes {
				match value {
					Some(v) => base_pending.insert(key.clone(), v.clone()),
					None => base_pending.remove(key.clone()),
				}
			}

			let mut flow_txn = FlowTransaction::transactional(
				read_version,
				Pending::new(),
				base_pending,
				primitive_query,
				state_query,
				self.catalog.clone(),
				interceptors,
			);

			for change in relevant {
				engine.process(&mut flow_txn, change, flow_id)?;
			}

			// Collect view changes emitted by SinkView for downstream flows.
			available_changes.extend(flow_txn.take_view_changes());

			// Merge pending view writes into the commit context.
			let flow_pending = flow_txn.take_pending();
			for (key, pw) in flow_pending.iter_sorted() {
				match pw {
					PendingWrite::Set(v) => ctx.pending_writes.push((key.clone(), Some(v.clone()))),
					PendingWrite::Remove => ctx.pending_writes.push((key.clone(), None)),
				}
			}
		}

		Ok(())
	}
}

/// Merge individual per-row changes into batched changes grouped by origin.
///
/// The transactional path tracks one `Change` per row (each with a single `Diff`),
/// while the deferred/CDC path batches all diffs from the same origin into a single
/// `Change` with one `Diff::Insert` / `Diff::Update` / `Diff::Remove` containing
/// multi-row `Columns`. Operators like hash-join group rows by join key within a
/// single `Diff`, so this deep merge is required for consistent output ordering.
fn merge_changes_by_origin(changes: &[Change], version: CommitVersion) -> Vec<Change> {
	let mut grouped: BTreeMap<PrimitiveId, Vec<Diff>> = BTreeMap::new();
	let mut non_primitive: Vec<Change> = Vec::new();

	for change in changes {
		match change.origin {
			ChangeOrigin::Primitive(id) => {
				grouped.entry(id).or_default().extend(change.diffs.iter().cloned());
			}
			_ => {
				let mut c = change.clone();
				c.version = version;
				non_primitive.push(c);
			}
		}
	}

	let mut result: Vec<Change> = grouped
		.into_iter()
		.map(|(id, diffs)| {
			// Merge diffs of the same type into single multi-row diffs,
			// matching the CDC producer's `merge_diffs` behavior.
			let merged = merge_diffs(diffs);
			Change {
				origin: ChangeOrigin::Primitive(id),
				diffs: merged,
				version,
			}
		})
		.collect();
	result.extend(non_primitive);
	result
}

/// Merge multiple single-row diffs into at most 3 multi-row diffs
/// (one Insert, one Update, one Remove) by appending Columns.
fn merge_diffs(diffs: Vec<Diff>) -> Vec<Diff> {
	let mut insert: Option<Diff> = None;
	let mut update: Option<Diff> = None;
	let mut remove: Option<Diff> = None;

	for diff in diffs {
		match diff {
			Diff::Insert {
				post,
			} => {
				if let Some(Diff::Insert {
					post: ref mut existing,
				}) = insert
				{
					let _ = existing.append_columns(post);
				} else {
					insert = Some(Diff::Insert {
						post,
					});
				}
			}
			Diff::Update {
				pre,
				post,
			} => {
				if let Some(Diff::Update {
					pre: ref mut existing_pre,
					post: ref mut existing_post,
				}) = update
				{
					let _ = existing_pre.append_columns(pre);
					let _ = existing_post.append_columns(post);
				} else {
					update = Some(Diff::Update {
						pre,
						post,
					});
				}
			}
			Diff::Remove {
				pre,
			} => {
				if let Some(Diff::Remove {
					pre: ref mut existing,
				}) = remove
				{
					let _ = existing.append_columns(pre);
				} else {
					remove = Some(Diff::Remove {
						pre,
					});
				}
			}
		}
	}

	let mut result = Vec::with_capacity(3);
	if let Some(d) = insert {
		result.push(d);
	}
	if let Some(d) = update {
		result.push(d);
	}
	if let Some(d) = remove {
		result.push(d);
	}
	result
}

/// Returns true if the given change is relevant to the given flow.
///
/// Uses the flow engine's `sources` map which records which primitives
/// (tables/views) each flow listens to.
fn flow_is_interested_in(change: &Change, flow_id: FlowId, engine: &FlowEngine) -> bool {
	if let ChangeOrigin::Primitive(source) = change.origin {
		engine.sources
			.get(&source)
			.map(|registrations| registrations.iter().any(|(fid, _)| *fid == flow_id))
			.unwrap_or(false)
	} else {
		false
	}
}

/// Post-commit interceptor that eagerly registers transactional flows.
///
/// When an admin transaction that creates a new flow commits, this interceptor
/// loads the flow DAG and registers it in the transactional `FlowEngine` so it
/// is available for the very next transaction's pre-commit phase — without
/// waiting for CDC polling to discover it.
pub struct TransactionalFlowPostCommitInterceptor {
	pub registrar: TransactionalFlowRegistrar,
}

impl PostCommitInterceptor for TransactionalFlowPostCommitInterceptor {
	fn intercept(&self, ctx: &mut PostCommitContext) -> Result<()> {
		for flow_change in &ctx.changes.flow_def {
			if flow_change.op == OperationType::Create {
				if let Some(flow_def) = &flow_change.post {
					if let Err(e) = self.registrar.try_register_by_id(flow_def.id) {
						warn!(
							flow_id = flow_def.id.0,
							error = %e,
							"failed to register transactional flow on commit"
						);
					}
				}
			}
		}
		Ok(())
	}
}
