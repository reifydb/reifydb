// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Enum-based dispatch for virtual tables
//!
//! This module provides `VTableImpl`, an enum that wraps all virtual table
//! implementations for static dispatch without trait objects.

use std::sync::Arc;

use reifydb_core::{interface::catalog::vtable::VTableDef, value::column::columns::Columns};
use reifydb_transaction::transaction::AsTransaction;
use reifydb_type::params::Params;

use super::{
	Batch, VTable, VTableContext,
	system::{
		cdc_consumers::CdcConsumers, column_policies::ColumnPolicies, columns::ColumnsTable,
		dictionaries::Dictionaries, dictionary_storage_stats::DictionaryStorageStats, enums::Enums,
		flow_edges::FlowEdges, flow_lags::FlowLags, flow_node_storage_stats::FlowNodeStorageStats,
		flow_node_types::FlowNodeTypes, flow_nodes::FlowNodes, flow_operator_inputs::FlowOperatorInputs,
		flow_operator_outputs::FlowOperatorOutputs, flow_operators::FlowOperators,
		flow_storage_stats::FlowStorageStats, flows::Flows, index_storage_stats::IndexStorageStats,
		namespaces::Namespaces, operator_retention_policies::OperatorRetentionPolicies,
		primary_key_columns::PrimaryKeyColumns, primary_keys::PrimaryKeys,
		primitive_retention_policies::PrimitiveRetentionPolicies,
		ringbuffer_storage_stats::RingBufferStorageStats, ringbuffers::RingBuffers,
		schema_fields::SchemaFields, schemas::Schemas, sequences::Sequences,
		table_storage_stats::TableStorageStats, tables::Tables, tables_virtual::TablesVirtual, types::Types,
		versions::Versions, view_storage_stats::ViewStorageStats, views::Views,
	},
};

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
	Schemas(Schemas),
	SchemaFields(SchemaFields),
	Enums(Enums),

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
			Self::Schemas(t) => &t.definition,
			Self::SchemaFields(t) => &t.definition,
			Self::Enums(t) => &t.definition,
			Self::UserDefined {
				def,
				..
			} => def,
		}
	}

	/// Initialize the virtual table iterator with context
	pub fn initialize<T: AsTransaction>(&mut self, txn: &mut T, ctx: VTableContext) -> crate::Result<()> {
		match self {
			Self::Sequences(t) => t.initialize(txn, ctx),
			Self::Namespaces(t) => t.initialize(txn, ctx),
			Self::Tables(t) => t.initialize(txn, ctx),
			Self::Views(t) => t.initialize(txn, ctx),
			Self::Flows(t) => t.initialize(txn, ctx),
			Self::FlowLags(t) => t.initialize(txn, ctx),
			Self::FlowNodes(t) => t.initialize(txn, ctx),
			Self::FlowEdges(t) => t.initialize(txn, ctx),
			Self::Columns(t) => t.initialize(txn, ctx),
			Self::PrimaryKeys(t) => t.initialize(txn, ctx),
			Self::PrimaryKeyColumns(t) => t.initialize(txn, ctx),
			Self::ColumnPolicies(t) => t.initialize(txn, ctx),
			Self::Versions(t) => t.initialize(txn, ctx),
			Self::PrimitiveRetentionPolicies(t) => t.initialize(txn, ctx),
			Self::OperatorRetentionPolicies(t) => t.initialize(txn, ctx),
			Self::CdcConsumers(t) => t.initialize(txn, ctx),
			Self::FlowOperators(t) => t.initialize(txn, ctx),
			Self::FlowOperatorInputs(t) => t.initialize(txn, ctx),
			Self::FlowOperatorOutputs(t) => t.initialize(txn, ctx),
			Self::Dictionaries(t) => t.initialize(txn, ctx),
			Self::TablesVirtual(t) => t.initialize(txn, ctx),
			Self::Types(t) => t.initialize(txn, ctx),
			Self::FlowNodeTypes(t) => t.initialize(txn, ctx),
			Self::RingBuffers(t) => t.initialize(txn, ctx),
			Self::TableStorageStats(t) => t.initialize(txn, ctx),
			Self::IndexStorageStats(t) => t.initialize(txn, ctx),
			Self::ViewStorageStats(t) => t.initialize(txn, ctx),
			Self::FlowStorageStats(t) => t.initialize(txn, ctx),
			Self::FlowNodeStorageStats(t) => t.initialize(txn, ctx),
			Self::RingBufferStorageStats(t) => t.initialize(txn, ctx),
			Self::DictionaryStorageStats(t) => t.initialize(txn, ctx),
			Self::Schemas(t) => t.initialize(txn, ctx),
			Self::SchemaFields(t) => t.initialize(txn, ctx),
			Self::Enums(t) => t.initialize(txn, ctx),
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
	pub fn next<T: AsTransaction>(&mut self, txn: &mut T) -> crate::Result<Option<Batch>> {
		match self {
			Self::Sequences(t) => t.next(txn),
			Self::Namespaces(t) => t.next(txn),
			Self::Tables(t) => t.next(txn),
			Self::Views(t) => t.next(txn),
			Self::Flows(t) => t.next(txn),
			Self::FlowLags(t) => t.next(txn),
			Self::FlowNodes(t) => t.next(txn),
			Self::FlowEdges(t) => t.next(txn),
			Self::Columns(t) => t.next(txn),
			Self::PrimaryKeys(t) => t.next(txn),
			Self::PrimaryKeyColumns(t) => t.next(txn),
			Self::ColumnPolicies(t) => t.next(txn),
			Self::Versions(t) => t.next(txn),
			Self::PrimitiveRetentionPolicies(t) => t.next(txn),
			Self::OperatorRetentionPolicies(t) => t.next(txn),
			Self::CdcConsumers(t) => t.next(txn),
			Self::FlowOperators(t) => t.next(txn),
			Self::FlowOperatorInputs(t) => t.next(txn),
			Self::FlowOperatorOutputs(t) => t.next(txn),
			Self::Dictionaries(t) => t.next(txn),
			Self::TablesVirtual(t) => t.next(txn),
			Self::Types(t) => t.next(txn),
			Self::FlowNodeTypes(t) => t.next(txn),
			Self::RingBuffers(t) => t.next(txn),
			Self::TableStorageStats(t) => t.next(txn),
			Self::IndexStorageStats(t) => t.next(txn),
			Self::ViewStorageStats(t) => t.next(txn),
			Self::FlowStorageStats(t) => t.next(txn),
			Self::FlowNodeStorageStats(t) => t.next(txn),
			Self::RingBufferStorageStats(t) => t.next(txn),
			Self::DictionaryStorageStats(t) => t.next(txn),
			Self::Schemas(t) => t.next(txn),
			Self::SchemaFields(t) => t.next(txn),
			Self::Enums(t) => t.next(txn),
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
