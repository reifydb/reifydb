// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Enum-based dispatch for virtual tables
//!
//! This module provides `VTableImpl`, an enum that wraps all virtual table
//! implementations for static dispatch without trait objects.

use std::sync::Arc;

use reifydb_core::{
	interface::{Batch, Params, QueryTransaction, VTableDef},
	value::column::Columns,
};

use super::{
	VTable, VTableContext,
	system::{
		CdcConsumers, ColumnPolicies, ColumnsTable, Dictionaries, DictionaryStorageStats, FlowEdges, FlowLags,
		FlowNodeStorageStats, FlowNodeTypes, FlowNodes, FlowOperatorInputs, FlowOperatorOutputs, FlowOperators,
		FlowStorageStats, Flows, IndexStorageStats, Namespaces, OperatorRetentionPolicies, PrimaryKeyColumns,
		PrimaryKeys, PrimitiveRetentionPolicies, RingBufferStorageStats, RingBuffers, Sequences,
		TableStorageStats, Tables, TablesVirtual, Types, Versions, ViewStorageStats, Views,
	},
};
use crate::transaction::MaterializedCatalogTransaction;

/// Callback type for user-defined virtual tables.
/// Returns column-oriented data directly.
pub type UserVTableDataFunction = Arc<dyn Fn(&Params) -> Columns + Send + Sync>;

/// Enum dispatch for all virtual table implementations.
///
/// This eliminates the need for `Box<dyn VTable>` trait objects by using
/// static dispatch via match expressions.
pub enum VTables {
	// System tables
	Sequences(Sequences),
	Namespaces(Namespaces),
	Tables(Tables),
	Views(Views),
	Flows(Flows),
	FlowLags(FlowLags),
	FlowNodes(FlowNodes),
	FlowEdges(FlowEdges),
	Columns(ColumnsTable),
	PrimaryKeys(PrimaryKeys),
	PrimaryKeyColumns(PrimaryKeyColumns),
	ColumnPolicies(ColumnPolicies),
	Versions(Versions),
	PrimitiveRetentionPolicies(PrimitiveRetentionPolicies),
	OperatorRetentionPolicies(OperatorRetentionPolicies),
	CdcConsumers(CdcConsumers),
	FlowOperators(FlowOperators),
	FlowOperatorInputs(FlowOperatorInputs),
	FlowOperatorOutputs(FlowOperatorOutputs),
	Dictionaries(Dictionaries),
	TablesVirtual(TablesVirtual),
	Types(Types),
	FlowNodeTypes(FlowNodeTypes),
	RingBuffers(RingBuffers),
	TableStorageStats(TableStorageStats),
	IndexStorageStats(IndexStorageStats),
	ViewStorageStats(ViewStorageStats),
	FlowStorageStats(FlowStorageStats),
	FlowNodeStorageStats(FlowNodeStorageStats),
	RingBufferStorageStats(RingBufferStorageStats),
	DictionaryStorageStats(DictionaryStorageStats),

	/// User-defined virtual table (callback-based)
	UserDefined {
		def: Arc<VTableDef>,
		data_fn: UserVTableDataFunction,
		/// Cached params from initialize, used in next()
		params: Option<Params>,
		exhausted: bool,
	},
}

impl VTables {
	/// Get the table definition
	pub fn definition(&self) -> &VTableDef {
		match self {
			Self::Sequences(t) => &t.definition,
			Self::Namespaces(t) => &t.definition,
			Self::Tables(t) => &t.definition,
			Self::Views(t) => &t.definition,
			Self::Flows(t) => &t.definition,
			Self::FlowLags(t) => &t.definition,
			Self::FlowNodes(t) => &t.definition,
			Self::FlowEdges(t) => &t.definition,
			Self::Columns(t) => &t.definition,
			Self::PrimaryKeys(t) => &t.definition,
			Self::PrimaryKeyColumns(t) => &t.definition,
			Self::ColumnPolicies(t) => &t.definition,
			Self::Versions(t) => &t.definition,
			Self::PrimitiveRetentionPolicies(t) => &t.definition,
			Self::OperatorRetentionPolicies(t) => &t.definition,
			Self::CdcConsumers(t) => &t.definition,
			Self::FlowOperators(t) => &t.definition,
			Self::FlowOperatorInputs(t) => &t.definition,
			Self::FlowOperatorOutputs(t) => &t.definition,
			Self::Dictionaries(t) => &t.definition,
			Self::TablesVirtual(t) => &t.definition,
			Self::Types(t) => &t.definition,
			Self::FlowNodeTypes(t) => &t.definition,
			Self::RingBuffers(t) => &t.definition,
			Self::TableStorageStats(t) => &t.definition,
			Self::IndexStorageStats(t) => &t.definition,
			Self::ViewStorageStats(t) => &t.definition,
			Self::FlowStorageStats(t) => &t.definition,
			Self::FlowNodeStorageStats(t) => &t.definition,
			Self::RingBufferStorageStats(t) => &t.definition,
			Self::DictionaryStorageStats(t) => &t.definition,
			Self::UserDefined {
				def,
				..
			} => def,
		}
	}

	/// Initialize the virtual table iterator with context
	pub async fn initialize<T: QueryTransaction + MaterializedCatalogTransaction>(
		&mut self,
		txn: &mut T,
		ctx: VTableContext,
	) -> crate::Result<()> {
		match self {
			Self::Sequences(t) => t.initialize(txn, ctx).await,
			Self::Namespaces(t) => t.initialize(txn, ctx).await,
			Self::Tables(t) => t.initialize(txn, ctx).await,
			Self::Views(t) => t.initialize(txn, ctx).await,
			Self::Flows(t) => t.initialize(txn, ctx).await,
			Self::FlowLags(t) => t.initialize(txn, ctx).await,
			Self::FlowNodes(t) => t.initialize(txn, ctx).await,
			Self::FlowEdges(t) => t.initialize(txn, ctx).await,
			Self::Columns(t) => t.initialize(txn, ctx).await,
			Self::PrimaryKeys(t) => t.initialize(txn, ctx).await,
			Self::PrimaryKeyColumns(t) => t.initialize(txn, ctx).await,
			Self::ColumnPolicies(t) => t.initialize(txn, ctx).await,
			Self::Versions(t) => t.initialize(txn, ctx).await,
			Self::PrimitiveRetentionPolicies(t) => t.initialize(txn, ctx).await,
			Self::OperatorRetentionPolicies(t) => t.initialize(txn, ctx).await,
			Self::CdcConsumers(t) => t.initialize(txn, ctx).await,
			Self::FlowOperators(t) => t.initialize(txn, ctx).await,
			Self::FlowOperatorInputs(t) => t.initialize(txn, ctx).await,
			Self::FlowOperatorOutputs(t) => t.initialize(txn, ctx).await,
			Self::Dictionaries(t) => t.initialize(txn, ctx).await,
			Self::TablesVirtual(t) => t.initialize(txn, ctx).await,
			Self::Types(t) => t.initialize(txn, ctx).await,
			Self::FlowNodeTypes(t) => t.initialize(txn, ctx).await,
			Self::RingBuffers(t) => t.initialize(txn, ctx).await,
			Self::TableStorageStats(t) => t.initialize(txn, ctx).await,
			Self::IndexStorageStats(t) => t.initialize(txn, ctx).await,
			Self::ViewStorageStats(t) => t.initialize(txn, ctx).await,
			Self::FlowStorageStats(t) => t.initialize(txn, ctx).await,
			Self::FlowNodeStorageStats(t) => t.initialize(txn, ctx).await,
			Self::RingBufferStorageStats(t) => t.initialize(txn, ctx).await,
			Self::DictionaryStorageStats(t) => t.initialize(txn, ctx).await,
			Self::UserDefined {
				params: stored_params,
				exhausted,
				..
			} => {
				// Store params for use in next()
				*stored_params = Some(match ctx {
					VTableContext::Basic {
						params,
					} => params,
					VTableContext::PushDown {
						params,
						..
					} => params,
				});
				*exhausted = false;
				Ok(())
			}
		}
	}

	/// Get the next batch of results (volcano iterator pattern)
	pub async fn next<T: QueryTransaction + MaterializedCatalogTransaction>(
		&mut self,
		txn: &mut T,
	) -> crate::Result<Option<Batch>> {
		match self {
			Self::Sequences(t) => t.next(txn).await,
			Self::Namespaces(t) => t.next(txn).await,
			Self::Tables(t) => t.next(txn).await,
			Self::Views(t) => t.next(txn).await,
			Self::Flows(t) => t.next(txn).await,
			Self::FlowLags(t) => t.next(txn).await,
			Self::FlowNodes(t) => t.next(txn).await,
			Self::FlowEdges(t) => t.next(txn).await,
			Self::Columns(t) => t.next(txn).await,
			Self::PrimaryKeys(t) => t.next(txn).await,
			Self::PrimaryKeyColumns(t) => t.next(txn).await,
			Self::ColumnPolicies(t) => t.next(txn).await,
			Self::Versions(t) => t.next(txn).await,
			Self::PrimitiveRetentionPolicies(t) => t.next(txn).await,
			Self::OperatorRetentionPolicies(t) => t.next(txn).await,
			Self::CdcConsumers(t) => t.next(txn).await,
			Self::FlowOperators(t) => t.next(txn).await,
			Self::FlowOperatorInputs(t) => t.next(txn).await,
			Self::FlowOperatorOutputs(t) => t.next(txn).await,
			Self::Dictionaries(t) => t.next(txn).await,
			Self::TablesVirtual(t) => t.next(txn).await,
			Self::Types(t) => t.next(txn).await,
			Self::FlowNodeTypes(t) => t.next(txn).await,
			Self::RingBuffers(t) => t.next(txn).await,
			Self::TableStorageStats(t) => t.next(txn).await,
			Self::IndexStorageStats(t) => t.next(txn).await,
			Self::ViewStorageStats(t) => t.next(txn).await,
			Self::FlowStorageStats(t) => t.next(txn).await,
			Self::FlowNodeStorageStats(t) => t.next(txn).await,
			Self::RingBufferStorageStats(t) => t.next(txn).await,
			Self::DictionaryStorageStats(t) => t.next(txn).await,
			Self::UserDefined {
				data_fn,
				params: stored_params,
				exhausted,
				..
			} => {
				if *exhausted {
					return Ok(None);
				}

				// Call user's data function which returns Columns directly
				let default_params = Params::default();
				let params_ref = stored_params.as_ref().unwrap_or(&default_params);
				let columns = data_fn(params_ref);

				*exhausted = true;
				Ok(Some(Batch {
					columns,
				}))
			}
		}
	}
}
