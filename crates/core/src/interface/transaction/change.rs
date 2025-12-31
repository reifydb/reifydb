// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use crate::interface::{
	DictionaryDef, DictionaryId, FlowDef, FlowId, NamespaceDef, NamespaceId, OperationType::Delete, RingBufferDef,
	RingBufferId, TableDef, TableId, TransactionId, ViewDef, ViewId,
};

pub trait TransactionalChanges:
	TransactionalDictionaryChanges
	+ TransactionalFlowChanges
	+ TransactionalNamespaceChanges
	+ TransactionalRingBufferChanges
	+ TransactionalTableChanges
	+ TransactionalViewChanges
{
}

pub trait TransactionalDictionaryChanges {
	fn find_dictionary(&self, id: DictionaryId) -> Option<&DictionaryDef>;

	fn find_dictionary_by_name(&self, namespace: NamespaceId, name: &str) -> Option<&DictionaryDef>;

	fn is_dictionary_deleted(&self, id: DictionaryId) -> bool;

	fn is_dictionary_deleted_by_name(&self, namespace: NamespaceId, name: &str) -> bool;
}

pub trait TransactionalNamespaceChanges {
	fn find_namespace(&self, id: NamespaceId) -> Option<&NamespaceDef>;

	fn find_namespace_by_name(&self, name: &str) -> Option<&NamespaceDef>;

	fn is_namespace_deleted(&self, id: NamespaceId) -> bool;

	fn is_namespace_deleted_by_name(&self, name: &str) -> bool;
}

pub trait TransactionalFlowChanges {
	fn find_flow(&self, id: FlowId) -> Option<&FlowDef>;

	fn find_flow_by_name(&self, namespace: NamespaceId, name: &str) -> Option<&FlowDef>;

	fn is_flow_deleted(&self, id: FlowId) -> bool;

	fn is_flow_deleted_by_name(&self, namespace: NamespaceId, name: &str) -> bool;
}

pub trait TransactionalTableChanges {
	fn find_table(&self, id: TableId) -> Option<&TableDef>;

	fn find_table_by_name(&self, namespace: NamespaceId, name: &str) -> Option<&TableDef>;

	fn is_table_deleted(&self, id: TableId) -> bool;

	fn is_table_deleted_by_name(&self, namespace: NamespaceId, name: &str) -> bool;
}

pub trait TransactionalRingBufferChanges {
	fn find_ringbuffer(&self, id: RingBufferId) -> Option<&RingBufferDef>;

	fn find_ringbuffer_by_name(&self, namespace: NamespaceId, name: &str) -> Option<&RingBufferDef>;

	fn is_ringbuffer_deleted(&self, id: RingBufferId) -> bool;

	fn is_ringbuffer_deleted_by_name(&self, namespace: NamespaceId, name: &str) -> bool;
}

pub trait TransactionalViewChanges {
	fn find_view(&self, id: ViewId) -> Option<&ViewDef>;

	fn find_view_by_name(&self, namespace: NamespaceId, name: &str) -> Option<&ViewDef>;

	fn is_view_deleted(&self, id: ViewId) -> bool;

	fn is_view_deleted_by_name(&self, namespace: NamespaceId, name: &str) -> bool;
}

#[derive(Default, Debug, Clone)]
pub struct TransactionalDefChanges {
	/// Transaction ID this change set belongs to
	pub txn_id: TransactionId,
	/// All dictionary definition changes in order (no coalescing)
	pub dictionary_def: Vec<Change<DictionaryDef>>,
	/// All flow definition changes in order (no coalescing)
	pub flow_def: Vec<Change<FlowDef>>,
	/// All namespace definition changes in order (no coalescing)
	pub namespace_def: Vec<Change<NamespaceDef>>,
	/// All ring buffer definition changes in order (no coalescing)
	pub ringbuffer_def: Vec<Change<RingBufferDef>>,
	/// All table definition changes in order (no coalescing)
	pub table_def: Vec<Change<TableDef>>,
	/// All view definition changes in order (no coalescing)
	pub view_def: Vec<Change<ViewDef>>,
	/// Order of operations for replay/rollback
	pub log: Vec<Operation>,
}

impl TransactionalDefChanges {
	pub fn add_dictionary_def_change(&mut self, change: Change<DictionaryDef>) {
		let id = change
			.post
			.as_ref()
			.or(change.pre.as_ref())
			.map(|d| d.id)
			.expect("Change must have either pre or post state");
		let op = change.op;
		self.dictionary_def.push(change);
		self.log.push(Operation::Dictionary {
			id,
			op,
		});
	}

	pub fn add_flow_def_change(&mut self, change: Change<FlowDef>) {
		let id = change
			.post
			.as_ref()
			.or(change.pre.as_ref())
			.map(|f| f.id)
			.expect("Change must have either pre or post state");
		let op = change.op;
		self.flow_def.push(change);
		self.log.push(Operation::Flow {
			id,
			op,
		});
	}

	pub fn add_namespace_def_change(&mut self, change: Change<NamespaceDef>) {
		let id = change
			.post
			.as_ref()
			.or(change.pre.as_ref())
			.map(|s| s.id)
			.expect("Change must have either pre or post state");
		let op = change.op;
		self.namespace_def.push(change);
		self.log.push(Operation::Namespace {
			id,
			op,
		});
	}

	pub fn add_ringbuffer_def_change(&mut self, change: Change<RingBufferDef>) {
		let id = change
			.post
			.as_ref()
			.or(change.pre.as_ref())
			.map(|rb| rb.id)
			.expect("Change must have either pre or post state");
		let op = change.op;
		self.ringbuffer_def.push(change);
		self.log.push(Operation::RingBuffer {
			id,
			op,
		});
	}

	pub fn add_table_def_change(&mut self, change: Change<TableDef>) {
		let id = change
			.post
			.as_ref()
			.or(change.pre.as_ref())
			.map(|t| t.id)
			.expect("Change must have either pre or post state");
		let op = change.op;
		self.table_def.push(change);
		self.log.push(Operation::Table {
			id,
			op,
		});
	}

	pub fn add_view_def_change(&mut self, change: Change<ViewDef>) {
		let id = change
			.post
			.as_ref()
			.or(change.pre.as_ref())
			.map(|v| v.id)
			.expect("Change must have either pre or post state");
		let op = change.op;
		self.view_def.push(change);
		self.log.push(Operation::View {
			id,
			op,
		});
	}
}

/// Represents a single change
#[derive(Debug, Clone)]
pub struct Change<T> {
	/// State before the change (None for CREATE)
	pub pre: Option<T>,

	/// State after the change (None for DELETE)
	pub post: Option<T>,

	/// Type of operation
	pub op: OperationType,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum OperationType {
	Create,
	Update,
	Delete,
}

/// Log entry for operation ordering
#[derive(Debug, Clone)]
pub enum Operation {
	Dictionary {
		id: DictionaryId,
		op: OperationType,
	},
	Flow {
		id: FlowId,
		op: OperationType,
	},
	Namespace {
		id: NamespaceId,
		op: OperationType,
	},
	RingBuffer {
		id: RingBufferId,
		op: OperationType,
	},
	Table {
		id: TableId,
		op: OperationType,
	},
	View {
		id: ViewId,
		op: OperationType,
	},
}

impl TransactionalDefChanges {
	pub fn new(txn_id: TransactionId) -> Self {
		Self {
			txn_id,
			dictionary_def: Vec::new(),
			flow_def: Vec::new(),
			namespace_def: Vec::new(),
			ringbuffer_def: Vec::new(),
			table_def: Vec::new(),
			view_def: Vec::new(),
			log: Vec::new(),
		}
	}

	/// Check if a table exists in this transaction's view
	pub fn table_def_exists(&self, id: TableId) -> bool {
		self.get_table_def(id).is_some()
	}

	/// Get current state of a table within this transaction
	pub fn get_table_def(&self, id: TableId) -> Option<&TableDef> {
		// Find the last change for this table ID
		for change in self.table_def.iter().rev() {
			if let Some(table) = &change.post {
				if table.id == id {
					return Some(table);
				}
			} else if let Some(table) = &change.pre {
				if table.id == id && change.op == Delete {
					// Table was deleted
					return None;
				}
			}
		}
		None
	}

	/// Check if a view exists in this transaction's view
	pub fn view_def_exists(&self, id: ViewId) -> bool {
		self.get_view_def(id).is_some()
	}

	/// Get current state of a view within this transaction
	pub fn get_view_def(&self, id: ViewId) -> Option<&ViewDef> {
		// Find the last change for this view ID
		for change in self.view_def.iter().rev() {
			if let Some(view) = &change.post {
				if view.id == id {
					return Some(view);
				}
			} else if let Some(view) = &change.pre {
				if view.id == id && change.op == Delete {
					// View was deleted
					return None;
				}
			}
		}
		None
	}

	/// Get all pending changes for commit
	pub fn get_pending_changes(&self) -> &[Operation] {
		&self.log
	}

	/// Get the transaction ID
	pub fn txn_id(&self) -> TransactionId {
		self.txn_id
	}

	/// Get namespace definition changes
	pub fn namespace_def(&self) -> &[Change<NamespaceDef>] {
		&self.namespace_def
	}

	/// Get table definition changes
	pub fn table_def(&self) -> &[Change<TableDef>] {
		&self.table_def
	}

	/// Get view definition changes
	pub fn view_def(&self) -> &[Change<ViewDef>] {
		&self.view_def
	}

	/// Clear all changes (for rollback)
	pub fn clear(&mut self) {
		self.dictionary_def.clear();
		self.flow_def.clear();
		self.namespace_def.clear();
		self.ringbuffer_def.clear();
		self.table_def.clear();
		self.view_def.clear();
		self.log.clear();
	}
}

/// Tracks a table row insertion for post-commit event emission
#[derive(Debug, Clone)]
pub struct TableRowInsertion {
	pub table_id: TableId,
	pub row_number: reifydb_type::RowNumber,
	pub encoded: crate::value::encoded::EncodedValues,
}

/// Tracks row changes across different entity types for post-commit event emission
#[derive(Debug, Clone)]
pub enum RowChange {
	/// A row was inserted into a table
	TableInsert(TableRowInsertion),
	// Future variants:
	// ViewInsert(ViewRowInsertion),
	// RingBufferInsert(RingBufferRowInsertion),
	// TableUpdate(TableRowUpdate),
	// TableDelete(TableRowDelete),
}
