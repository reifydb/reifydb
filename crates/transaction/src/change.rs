// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use OperationType::Delete;
use reifydb_core::interface::catalog::{
	dictionary::DictionaryDef,
	flow::{FlowDef, FlowId},
	handler::HandlerDef,
	id::{HandlerId, NamespaceId, ProcedureId, RingBufferId, SeriesId, SubscriptionId, TableId, ViewId},
	namespace::NamespaceDef,
	procedure::ProcedureDef,
	ringbuffer::RingBufferDef,
	security_policy::{SecurityPolicyDef, SecurityPolicyId},
	series::SeriesDef,
	subscription::SubscriptionDef,
	sumtype::SumTypeDef,
	table::TableDef,
	user::{RoleDef, RoleId, UserDef, UserId, UserRoleDef},
	view::ViewDef,
};
use reifydb_type::value::{dictionary::DictionaryId, sumtype::SumTypeId};

use crate::TransactionId;

pub trait TransactionalChanges:
	TransactionalDictionaryChanges
	+ TransactionalFlowChanges
	+ TransactionalHandlerChanges
	+ TransactionalNamespaceChanges
	+ TransactionalProcedureChanges
	+ TransactionalRingBufferChanges
	+ TransactionalRoleChanges
	+ TransactionalSecurityPolicyChanges
	+ TransactionalSeriesChanges
	+ TransactionalSubscriptionChanges
	+ TransactionalSumTypeChanges
	+ TransactionalTableChanges
	+ TransactionalUserChanges
	+ TransactionalUserRoleChanges
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

pub trait TransactionalProcedureChanges {
	fn find_procedure(&self, id: ProcedureId) -> Option<&ProcedureDef>;

	fn find_procedure_by_name(&self, namespace: NamespaceId, name: &str) -> Option<&ProcedureDef>;

	fn is_procedure_deleted(&self, id: ProcedureId) -> bool;

	fn is_procedure_deleted_by_name(&self, namespace: NamespaceId, name: &str) -> bool;
}

pub trait TransactionalRingBufferChanges {
	fn find_ringbuffer(&self, id: RingBufferId) -> Option<&RingBufferDef>;

	fn find_ringbuffer_by_name(&self, namespace: NamespaceId, name: &str) -> Option<&RingBufferDef>;

	fn is_ringbuffer_deleted(&self, id: RingBufferId) -> bool;

	fn is_ringbuffer_deleted_by_name(&self, namespace: NamespaceId, name: &str) -> bool;
}

pub trait TransactionalSeriesChanges {
	fn find_series(&self, id: SeriesId) -> Option<&SeriesDef>;

	fn find_series_by_name(&self, namespace: NamespaceId, name: &str) -> Option<&SeriesDef>;

	fn is_series_deleted(&self, id: SeriesId) -> bool;

	fn is_series_deleted_by_name(&self, namespace: NamespaceId, name: &str) -> bool;
}

pub trait TransactionalViewChanges {
	fn find_view(&self, id: ViewId) -> Option<&ViewDef>;

	fn find_view_by_name(&self, namespace: NamespaceId, name: &str) -> Option<&ViewDef>;

	fn is_view_deleted(&self, id: ViewId) -> bool;

	fn is_view_deleted_by_name(&self, namespace: NamespaceId, name: &str) -> bool;
}

/// Trait for querying subscription changes within a transaction.
/// Note: Subscriptions do NOT have names - they are identified only by ID.
pub trait TransactionalSumTypeChanges {
	fn find_sumtype(&self, id: SumTypeId) -> Option<&SumTypeDef>;

	fn find_sumtype_by_name(&self, namespace: NamespaceId, name: &str) -> Option<&SumTypeDef>;

	fn is_sumtype_deleted(&self, id: SumTypeId) -> bool;

	fn is_sumtype_deleted_by_name(&self, namespace: NamespaceId, name: &str) -> bool;
}

pub trait TransactionalSubscriptionChanges {
	fn find_subscription(&self, id: SubscriptionId) -> Option<&SubscriptionDef>;

	fn is_subscription_deleted(&self, id: SubscriptionId) -> bool;
}

pub trait TransactionalHandlerChanges {
	fn find_handler_by_id(&self, id: HandlerId) -> Option<&HandlerDef>;

	fn find_handler_by_name(&self, namespace: NamespaceId, name: &str) -> Option<&HandlerDef>;

	fn is_handler_deleted_by_name(&self, namespace: NamespaceId, name: &str) -> bool;
}

pub trait TransactionalUserChanges {
	fn find_user(&self, id: UserId) -> Option<&UserDef>;

	fn find_user_by_name(&self, name: &str) -> Option<&UserDef>;

	fn is_user_deleted(&self, id: UserId) -> bool;

	fn is_user_deleted_by_name(&self, name: &str) -> bool;
}

pub trait TransactionalRoleChanges {
	fn find_role(&self, id: RoleId) -> Option<&RoleDef>;

	fn find_role_by_name(&self, name: &str) -> Option<&RoleDef>;

	fn is_role_deleted(&self, id: RoleId) -> bool;

	fn is_role_deleted_by_name(&self, name: &str) -> bool;
}

pub trait TransactionalUserRoleChanges {
	fn find_user_role(&self, user: UserId, role: RoleId) -> Option<&UserRoleDef>;

	fn is_user_role_deleted(&self, user: UserId, role: RoleId) -> bool;
}

pub trait TransactionalSecurityPolicyChanges {
	fn find_security_policy(&self, id: SecurityPolicyId) -> Option<&SecurityPolicyDef>;

	fn find_security_policy_by_name(&self, name: &str) -> Option<&SecurityPolicyDef>;

	fn is_security_policy_deleted(&self, id: SecurityPolicyId) -> bool;

	fn is_security_policy_deleted_by_name(&self, name: &str) -> bool;
}

#[derive(Default, Debug, Clone)]
pub struct TransactionalDefChanges {
	/// Transaction ID this change set belongs to
	pub txn_id: TransactionId,
	/// All dictionary definition changes in order (no coalescing)
	pub dictionary_def: Vec<Change<DictionaryDef>>,
	/// All flow definition changes in order (no coalescing)
	pub flow_def: Vec<Change<FlowDef>>,
	/// All handler definition changes in order (no coalescing)
	pub handler_def: Vec<Change<HandlerDef>>,
	/// All namespace definition changes in order (no coalescing)
	pub namespace_def: Vec<Change<NamespaceDef>>,
	/// All procedure definition changes in order (no coalescing)
	pub procedure_def: Vec<Change<ProcedureDef>>,
	/// All ring buffer definition changes in order (no coalescing)
	pub ringbuffer_def: Vec<Change<RingBufferDef>>,
	/// All series definition changes in order (no coalescing)
	pub series_def: Vec<Change<SeriesDef>>,
	/// All subscription definition changes in order (no coalescing)
	pub sumtype_def: Vec<Change<SumTypeDef>>,
	pub subscription_def: Vec<Change<SubscriptionDef>>,
	/// All table definition changes in order (no coalescing)
	pub table_def: Vec<Change<TableDef>>,
	/// All user definition changes in order (no coalescing)
	pub user_def: Vec<Change<UserDef>>,
	/// All role definition changes in order (no coalescing)
	pub role_def: Vec<Change<RoleDef>>,
	/// All user-role definition changes in order (no coalescing)
	pub user_role_def: Vec<Change<UserRoleDef>>,
	/// All security policy definition changes in order (no coalescing)
	pub security_policy_def: Vec<Change<SecurityPolicyDef>>,
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

	pub fn add_handler_def_change(&mut self, change: Change<HandlerDef>) {
		let id = change
			.post
			.as_ref()
			.or(change.pre.as_ref())
			.map(|h| h.id)
			.expect("Change must have either pre or post state");
		let op = change.op;
		self.handler_def.push(change);
		self.log.push(Operation::Handler {
			id,
			op,
		});
	}

	pub fn add_procedure_def_change(&mut self, change: Change<ProcedureDef>) {
		let id = change
			.post
			.as_ref()
			.or(change.pre.as_ref())
			.map(|p| p.id)
			.expect("Change must have either pre or post state");
		let op = change.op;
		self.procedure_def.push(change);
		self.log.push(Operation::Procedure {
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

	pub fn add_series_def_change(&mut self, change: Change<SeriesDef>) {
		let id = change
			.post
			.as_ref()
			.or(change.pre.as_ref())
			.map(|s| s.id)
			.expect("Change must have either pre or post state");
		let op = change.op;
		self.series_def.push(change);
		self.log.push(Operation::Series {
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

	pub fn add_sumtype_def_change(&mut self, change: Change<SumTypeDef>) {
		let id = change
			.post
			.as_ref()
			.or(change.pre.as_ref())
			.map(|s| s.id)
			.expect("Change must have either pre or post state");
		let op = change.op;
		self.sumtype_def.push(change);
		self.log.push(Operation::SumType {
			id,
			op,
		});
	}

	pub fn add_subscription_def_change(&mut self, change: Change<SubscriptionDef>) {
		let id = change
			.post
			.as_ref()
			.or(change.pre.as_ref())
			.map(|s| s.id)
			.expect("Change must have either pre or post state");
		let op = change.op;
		self.subscription_def.push(change);
		self.log.push(Operation::Subscription {
			id,
			op,
		});
	}

	pub fn add_user_def_change(&mut self, change: Change<UserDef>) {
		let id = change
			.post
			.as_ref()
			.or(change.pre.as_ref())
			.map(|u| u.id)
			.expect("Change must have either pre or post state");
		let op = change.op;
		self.user_def.push(change);
		self.log.push(Operation::User {
			id,
			op,
		});
	}

	pub fn add_role_def_change(&mut self, change: Change<RoleDef>) {
		let id = change
			.post
			.as_ref()
			.or(change.pre.as_ref())
			.map(|r| r.id)
			.expect("Change must have either pre or post state");
		let op = change.op;
		self.role_def.push(change);
		self.log.push(Operation::Role {
			id,
			op,
		});
	}

	pub fn add_user_role_def_change(&mut self, change: Change<UserRoleDef>) {
		let user = change
			.post
			.as_ref()
			.or(change.pre.as_ref())
			.map(|ur| ur.user_id)
			.expect("Change must have either pre or post state");
		let role = change
			.post
			.as_ref()
			.or(change.pre.as_ref())
			.map(|ur| ur.role_id)
			.expect("Change must have either pre or post state");
		let op = change.op;
		self.user_role_def.push(change);
		self.log.push(Operation::UserRole {
			user,
			role,
			op,
		});
	}

	pub fn add_security_policy_def_change(&mut self, change: Change<SecurityPolicyDef>) {
		let id = change
			.post
			.as_ref()
			.or(change.pre.as_ref())
			.map(|p| p.id)
			.expect("Change must have either pre or post state");
		let op = change.op;
		self.security_policy_def.push(change);
		self.log.push(Operation::SecurityPolicy {
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
	Handler {
		id: HandlerId,
		op: OperationType,
	},
	Namespace {
		id: NamespaceId,
		op: OperationType,
	},
	Procedure {
		id: ProcedureId,
		op: OperationType,
	},
	RingBuffer {
		id: RingBufferId,
		op: OperationType,
	},
	Series {
		id: SeriesId,
		op: OperationType,
	},
	SumType {
		id: SumTypeId,
		op: OperationType,
	},
	Subscription {
		id: SubscriptionId,
		op: OperationType,
	},
	Table {
		id: TableId,
		op: OperationType,
	},
	User {
		id: UserId,
		op: OperationType,
	},
	Role {
		id: RoleId,
		op: OperationType,
	},
	UserRole {
		user: UserId,
		role: RoleId,
		op: OperationType,
	},
	SecurityPolicy {
		id: SecurityPolicyId,
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
			handler_def: Vec::new(),
			namespace_def: Vec::new(),
			procedure_def: Vec::new(),
			ringbuffer_def: Vec::new(),
			series_def: Vec::new(),
			sumtype_def: Vec::new(),
			subscription_def: Vec::new(),
			table_def: Vec::new(),
			user_def: Vec::new(),
			role_def: Vec::new(),
			user_role_def: Vec::new(),
			security_policy_def: Vec::new(),
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
		self.handler_def.clear();
		self.namespace_def.clear();
		self.procedure_def.clear();
		self.ringbuffer_def.clear();
		self.series_def.clear();
		self.sumtype_def.clear();
		self.subscription_def.clear();
		self.table_def.clear();
		self.user_def.clear();
		self.role_def.clear();
		self.user_role_def.clear();
		self.security_policy_def.clear();
		self.view_def.clear();
		self.log.clear();
	}
}

/// Tracks a table row insertion for post-commit event emission
#[derive(Debug, Clone)]
pub struct TableRowInsertion {
	pub table_id: TableId,
	pub row_number: reifydb_type::value::row_number::RowNumber,
	pub encoded: reifydb_core::encoded::encoded::EncodedValues,
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
