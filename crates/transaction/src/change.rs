// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use OperationType::Delete;
use reifydb_core::{
	encoded::row::EncodedRow,
	interface::catalog::{
		authentication::{Authentication, AuthenticationId},
		dictionary::Dictionary,
		flow::{Flow, FlowId},
		handler::Handler,
		id::{
			HandlerId, MigrationEventId, MigrationId, NamespaceId, ProcedureId, RingBufferId, SeriesId,
			SinkId, SourceId, SubscriptionId, TableId, TestId, ViewId,
		},
		identity::{GrantedRole, Identity, Role, RoleId},
		migration::{Migration, MigrationEvent},
		namespace::Namespace,
		policy::{Policy, PolicyId},
		procedure::Procedure,
		ringbuffer::RingBuffer,
		series::Series,
		sink::Sink,
		source::Source,
		subscription::Subscription,
		sumtype::SumType,
		table::Table,
		test::Test,
		view::View,
	},
};
use reifydb_type::value::{
	Value, dictionary::DictionaryId, identity::IdentityId, row_number::RowNumber, sumtype::SumTypeId,
};

use crate::TransactionId;

pub trait TransactionalChanges:
	TransactionalDictionaryChanges
	+ TransactionalFlowChanges
	+ TransactionalHandlerChanges
	+ TransactionalMigrationChanges
	+ TransactionalNamespaceChanges
	+ TransactionalProcedureChanges
	+ TransactionalRingBufferChanges
	+ TransactionalRoleChanges
	+ TransactionalPolicyChanges
	+ TransactionalSeriesChanges
	+ TransactionalSinkChanges
	+ TransactionalSourceChanges
	+ TransactionalSubscriptionChanges
	+ TransactionalSumTypeChanges
	+ TransactionalTableChanges
	+ TransactionalTestChanges
	+ TransactionalAuthenticationChanges
	+ TransactionalIdentityChanges
	+ TransactionalGrantedRoleChanges
	+ TransactionalViewChanges
{
}

pub trait TransactionalDictionaryChanges {
	fn find_dictionary(&self, id: DictionaryId) -> Option<&Dictionary>;

	fn find_dictionary_by_name(&self, namespace: NamespaceId, name: &str) -> Option<&Dictionary>;

	fn is_dictionary_deleted(&self, id: DictionaryId) -> bool;

	fn is_dictionary_deleted_by_name(&self, namespace: NamespaceId, name: &str) -> bool;
}

pub trait TransactionalNamespaceChanges {
	fn find_namespace(&self, id: NamespaceId) -> Option<&Namespace>;

	fn find_namespace_by_name(&self, name: &str) -> Option<&Namespace>;

	fn is_namespace_deleted(&self, id: NamespaceId) -> bool;

	fn is_namespace_deleted_by_name(&self, name: &str) -> bool;
}

pub trait TransactionalFlowChanges {
	fn find_flow(&self, id: FlowId) -> Option<&Flow>;

	fn find_flow_by_name(&self, namespace: NamespaceId, name: &str) -> Option<&Flow>;

	fn is_flow_deleted(&self, id: FlowId) -> bool;

	fn is_flow_deleted_by_name(&self, namespace: NamespaceId, name: &str) -> bool;
}

pub trait TransactionalTableChanges {
	fn find_table(&self, id: TableId) -> Option<&Table>;

	fn find_table_by_name(&self, namespace: NamespaceId, name: &str) -> Option<&Table>;

	fn is_table_deleted(&self, id: TableId) -> bool;

	fn is_table_deleted_by_name(&self, namespace: NamespaceId, name: &str) -> bool;
}

pub trait TransactionalProcedureChanges {
	fn find_procedure(&self, id: ProcedureId) -> Option<&Procedure>;

	fn find_procedure_by_name(&self, namespace: NamespaceId, name: &str) -> Option<&Procedure>;

	fn is_procedure_deleted(&self, id: ProcedureId) -> bool;

	fn is_procedure_deleted_by_name(&self, namespace: NamespaceId, name: &str) -> bool;
}

pub trait TransactionalTestChanges {
	fn find_test(&self, id: TestId) -> Option<&Test>;

	fn find_test_by_name(&self, namespace: NamespaceId, name: &str) -> Option<&Test>;

	fn is_test_deleted(&self, id: TestId) -> bool;

	fn is_test_deleted_by_name(&self, namespace: NamespaceId, name: &str) -> bool;
}

pub trait TransactionalRingBufferChanges {
	fn find_ringbuffer(&self, id: RingBufferId) -> Option<&RingBuffer>;

	fn find_ringbuffer_by_name(&self, namespace: NamespaceId, name: &str) -> Option<&RingBuffer>;

	fn is_ringbuffer_deleted(&self, id: RingBufferId) -> bool;

	fn is_ringbuffer_deleted_by_name(&self, namespace: NamespaceId, name: &str) -> bool;
}

pub trait TransactionalSeriesChanges {
	fn find_series(&self, id: SeriesId) -> Option<&Series>;

	fn find_series_by_name(&self, namespace: NamespaceId, name: &str) -> Option<&Series>;

	fn is_series_deleted(&self, id: SeriesId) -> bool;

	fn is_series_deleted_by_name(&self, namespace: NamespaceId, name: &str) -> bool;
}

pub trait TransactionalViewChanges {
	fn find_view(&self, id: ViewId) -> Option<&View>;

	fn find_view_by_name(&self, namespace: NamespaceId, name: &str) -> Option<&View>;

	fn is_view_deleted(&self, id: ViewId) -> bool;

	fn is_view_deleted_by_name(&self, namespace: NamespaceId, name: &str) -> bool;
}

/// Trait for querying subscription changes within a transaction.
/// Note: Subscriptions do NOT have names - they are identified only by ID.
pub trait TransactionalSumTypeChanges {
	fn find_sumtype(&self, id: SumTypeId) -> Option<&SumType>;

	fn find_sumtype_by_name(&self, namespace: NamespaceId, name: &str) -> Option<&SumType>;

	fn is_sumtype_deleted(&self, id: SumTypeId) -> bool;

	fn is_sumtype_deleted_by_name(&self, namespace: NamespaceId, name: &str) -> bool;
}

pub trait TransactionalSubscriptionChanges {
	fn find_subscription(&self, id: SubscriptionId) -> Option<&Subscription>;

	fn is_subscription_deleted(&self, id: SubscriptionId) -> bool;
}

pub trait TransactionalHandlerChanges {
	fn find_handler_by_id(&self, id: HandlerId) -> Option<&Handler>;

	fn find_handler_by_name(&self, namespace: NamespaceId, name: &str) -> Option<&Handler>;

	fn is_handler_deleted_by_name(&self, namespace: NamespaceId, name: &str) -> bool;
}

pub trait TransactionalIdentityChanges {
	fn find_identity(&self, id: IdentityId) -> Option<&Identity>;

	fn find_identity_by_name(&self, name: &str) -> Option<&Identity>;

	fn is_identity_deleted(&self, id: IdentityId) -> bool;

	fn is_identity_deleted_by_name(&self, name: &str) -> bool;
}

pub trait TransactionalRoleChanges {
	fn find_role(&self, id: RoleId) -> Option<&Role>;

	fn find_role_by_name(&self, name: &str) -> Option<&Role>;

	fn is_role_deleted(&self, id: RoleId) -> bool;

	fn is_role_deleted_by_name(&self, name: &str) -> bool;
}

pub trait TransactionalAuthenticationChanges {
	fn find_authentication(&self, id: AuthenticationId) -> Option<&Authentication>;

	fn find_authentication_by_identity_and_method(
		&self,
		identity: IdentityId,
		method: &str,
	) -> Option<&Authentication>;

	fn is_authentication_deleted(&self, id: AuthenticationId) -> bool;
}

pub trait TransactionalGrantedRoleChanges {
	fn find_granted_role(&self, identity: IdentityId, role: RoleId) -> Option<&GrantedRole>;

	fn find_granted_roles_for_identity(&self, identity: IdentityId) -> Vec<&GrantedRole>;

	fn is_granted_role_deleted(&self, identity: IdentityId, role: RoleId) -> bool;
}

pub trait TransactionalPolicyChanges {
	fn find_policy(&self, id: PolicyId) -> Option<&Policy>;

	fn find_policy_by_name(&self, name: &str) -> Option<&Policy>;

	fn is_policy_deleted(&self, id: PolicyId) -> bool;

	fn is_policy_deleted_by_name(&self, name: &str) -> bool;
}

pub trait TransactionalSourceChanges {
	fn find_source(&self, id: SourceId) -> Option<&Source>;

	fn find_source_by_name(&self, namespace: NamespaceId, name: &str) -> Option<&Source>;

	fn is_source_deleted(&self, id: SourceId) -> bool;

	fn is_source_deleted_by_name(&self, namespace: NamespaceId, name: &str) -> bool;
}

pub trait TransactionalSinkChanges {
	fn find_sink(&self, id: SinkId) -> Option<&Sink>;

	fn find_sink_by_name(&self, namespace: NamespaceId, name: &str) -> Option<&Sink>;

	fn is_sink_deleted(&self, id: SinkId) -> bool;

	fn is_sink_deleted_by_name(&self, namespace: NamespaceId, name: &str) -> bool;
}

pub trait TransactionalMigrationChanges {
	fn find_migration(&self, id: MigrationId) -> Option<&Migration>;

	fn find_migration_by_name(&self, name: &str) -> Option<&Migration>;

	fn is_migration_deleted(&self, id: MigrationId) -> bool;

	fn is_migration_deleted_by_name(&self, name: &str) -> bool;
}

#[derive(Default, Debug, Clone)]
pub struct TransactionalDefChanges {
	/// Transaction ID this change set belongs to
	pub txn_id: TransactionId,
	/// Config key/value changes to be applied post-commit with the commit version
	pub config_changes: Vec<(String, Value)>,
	/// All dictionary definition changes in order (no coalescing)
	pub dictionary: Vec<Change<Dictionary>>,
	/// All flow definition changes in order (no coalescing)
	pub flow: Vec<Change<Flow>>,
	/// All handler definition changes in order (no coalescing)
	pub handler: Vec<Change<Handler>>,
	/// All migration definition changes in order (no coalescing)
	pub migration: Vec<Change<Migration>>,
	/// All migration event changes in order (no coalescing)
	pub migration_event: Vec<Change<MigrationEvent>>,
	/// All namespace definition changes in order (no coalescing)
	pub namespace: Vec<Change<Namespace>>,
	/// All procedure definition changes in order (no coalescing)
	pub procedure: Vec<Change<Procedure>>,
	/// All ring buffer definition changes in order (no coalescing)
	pub ringbuffer: Vec<Change<RingBuffer>>,
	/// All series definition changes in order (no coalescing)
	pub series: Vec<Change<Series>>,
	/// All sink definition changes in order (no coalescing)
	pub sink: Vec<Change<Sink>>,
	/// All source definition changes in order (no coalescing)
	pub source: Vec<Change<Source>>,
	/// All subscription definition changes in order (no coalescing)
	pub sumtype: Vec<Change<SumType>>,
	pub subscription: Vec<Change<Subscription>>,
	/// All test definition changes in order (no coalescing)
	pub test: Vec<Change<Test>>,
	/// All table definition changes in order (no coalescing)
	pub table: Vec<Change<Table>>,
	/// All identity definition changes in order (no coalescing)
	pub identity: Vec<Change<Identity>>,
	/// All authentication definition changes in order (no coalescing)
	pub authentication: Vec<Change<Authentication>>,
	/// All role definition changes in order (no coalescing)
	pub role: Vec<Change<Role>>,
	/// All identity-role definition changes in order (no coalescing)
	pub granted_role: Vec<Change<GrantedRole>>,
	/// All policy definition changes in order (no coalescing)
	pub policy: Vec<Change<Policy>>,
	/// All view definition changes in order (no coalescing)
	pub view: Vec<Change<View>>,
	/// Order of operations for replay/rollback
	pub log: Vec<Operation>,
}

impl TransactionalDefChanges {
	pub fn add_config_change(&mut self, key: String, value: Value) {
		self.config_changes.push((key, value));
	}

	pub fn add_dictionary_change(&mut self, change: Change<Dictionary>) {
		let id = change
			.post
			.as_ref()
			.or(change.pre.as_ref())
			.map(|d| d.id)
			.expect("Change must have either pre or post state");
		let op = change.op;
		self.dictionary.push(change);
		self.log.push(Operation::Dictionary {
			id,
			op,
		});
	}

	pub fn add_flow_change(&mut self, change: Change<Flow>) {
		let id = change
			.post
			.as_ref()
			.or(change.pre.as_ref())
			.map(|f| f.id)
			.expect("Change must have either pre or post state");
		let op = change.op;
		self.flow.push(change);
		self.log.push(Operation::Flow {
			id,
			op,
		});
	}

	pub fn add_namespace_change(&mut self, change: Change<Namespace>) {
		let id = change
			.post
			.as_ref()
			.or(change.pre.as_ref())
			.map(|s| s.id())
			.expect("Change must have either pre or post state");
		let op = change.op;
		self.namespace.push(change);
		self.log.push(Operation::Namespace {
			id,
			op,
		});
	}

	pub fn add_handler_change(&mut self, change: Change<Handler>) {
		let id = change
			.post
			.as_ref()
			.or(change.pre.as_ref())
			.map(|h| h.id)
			.expect("Change must have either pre or post state");
		let op = change.op;
		self.handler.push(change);
		self.log.push(Operation::Handler {
			id,
			op,
		});
	}

	pub fn add_migration_change(&mut self, change: Change<Migration>) {
		let id = change
			.post
			.as_ref()
			.or(change.pre.as_ref())
			.map(|m| m.id)
			.expect("Change must have either pre or post state");
		let op = change.op;
		self.migration.push(change);
		self.log.push(Operation::Migration {
			id,
			op,
		});
	}

	pub fn add_migration_event_change(&mut self, change: Change<MigrationEvent>) {
		let id = change
			.post
			.as_ref()
			.or(change.pre.as_ref())
			.map(|e| e.id)
			.expect("Change must have either pre or post state");
		let op = change.op;
		self.migration_event.push(change);
		self.log.push(Operation::MigrationEvent {
			id,
			op,
		});
	}

	pub fn add_procedure_change(&mut self, change: Change<Procedure>) {
		let id = change
			.post
			.as_ref()
			.or(change.pre.as_ref())
			.map(|p| p.id)
			.expect("Change must have either pre or post state");
		let op = change.op;
		self.procedure.push(change);
		self.log.push(Operation::Procedure {
			id,
			op,
		});
	}

	pub fn add_test_change(&mut self, change: Change<Test>) {
		let id = change
			.post
			.as_ref()
			.or(change.pre.as_ref())
			.map(|t| t.id)
			.expect("Change must have either pre or post state");
		let op = change.op;
		self.test.push(change);
		self.log.push(Operation::Test {
			id,
			op,
		});
	}

	pub fn add_ringbuffer_change(&mut self, change: Change<RingBuffer>) {
		let id = change
			.post
			.as_ref()
			.or(change.pre.as_ref())
			.map(|rb| rb.id)
			.expect("Change must have either pre or post state");
		let op = change.op;
		self.ringbuffer.push(change);
		self.log.push(Operation::RingBuffer {
			id,
			op,
		});
	}

	pub fn add_series_change(&mut self, change: Change<Series>) {
		let id = change
			.post
			.as_ref()
			.or(change.pre.as_ref())
			.map(|s| s.id)
			.expect("Change must have either pre or post state");
		let op = change.op;
		self.series.push(change);
		self.log.push(Operation::Series {
			id,
			op,
		});
	}

	pub fn add_sink_change(&mut self, change: Change<Sink>) {
		let id = change
			.post
			.as_ref()
			.or(change.pre.as_ref())
			.map(|s| s.id)
			.expect("Change must have either pre or post state");
		let op = change.op;
		self.sink.push(change);
		self.log.push(Operation::Sink {
			id,
			op,
		});
	}

	pub fn add_source_change(&mut self, change: Change<Source>) {
		let id = change
			.post
			.as_ref()
			.or(change.pre.as_ref())
			.map(|s| s.id)
			.expect("Change must have either pre or post state");
		let op = change.op;
		self.source.push(change);
		self.log.push(Operation::Source {
			id,
			op,
		});
	}

	pub fn add_table_change(&mut self, change: Change<Table>) {
		let id = change
			.post
			.as_ref()
			.or(change.pre.as_ref())
			.map(|t| t.id)
			.expect("Change must have either pre or post state");
		let op = change.op;
		self.table.push(change);
		self.log.push(Operation::Table {
			id,
			op,
		});
	}

	pub fn add_view_change(&mut self, change: Change<View>) {
		let id = change
			.post
			.as_ref()
			.or(change.pre.as_ref())
			.map(|v| v.id())
			.expect("Change must have either pre or post state");
		let op = change.op;
		self.view.push(change);
		self.log.push(Operation::View {
			id,
			op,
		});
	}

	pub fn add_sumtype_change(&mut self, change: Change<SumType>) {
		let id = change
			.post
			.as_ref()
			.or(change.pre.as_ref())
			.map(|s| s.id)
			.expect("Change must have either pre or post state");
		let op = change.op;
		self.sumtype.push(change);
		self.log.push(Operation::SumType {
			id,
			op,
		});
	}

	pub fn add_subscription_change(&mut self, change: Change<Subscription>) {
		let id = change
			.post
			.as_ref()
			.or(change.pre.as_ref())
			.map(|s| s.id)
			.expect("Change must have either pre or post state");
		let op = change.op;
		self.subscription.push(change);
		self.log.push(Operation::Subscription {
			id,
			op,
		});
	}

	pub fn add_identity_change(&mut self, change: Change<Identity>) {
		let id = change
			.post
			.as_ref()
			.or(change.pre.as_ref())
			.map(|u| u.id)
			.expect("Change must have either pre or post state");
		let op = change.op;
		self.identity.push(change);
		self.log.push(Operation::Identity {
			id,
			op,
		});
	}

	pub fn add_role_change(&mut self, change: Change<Role>) {
		let id = change
			.post
			.as_ref()
			.or(change.pre.as_ref())
			.map(|r| r.id)
			.expect("Change must have either pre or post state");
		let op = change.op;
		self.role.push(change);
		self.log.push(Operation::Role {
			id,
			op,
		});
	}

	pub fn add_granted_role_change(&mut self, change: Change<GrantedRole>) {
		let identity = change
			.post
			.as_ref()
			.or(change.pre.as_ref())
			.map(|ur| ur.identity)
			.expect("Change must have either pre or post state");
		let role = change
			.post
			.as_ref()
			.or(change.pre.as_ref())
			.map(|ur| ur.role_id)
			.expect("Change must have either pre or post state");
		let op = change.op;
		self.granted_role.push(change);
		self.log.push(Operation::GrantedRole {
			identity,
			role,
			op,
		});
	}

	pub fn add_authentication_change(&mut self, change: Change<Authentication>) {
		let id = change
			.post
			.as_ref()
			.or(change.pre.as_ref())
			.map(|a| a.id)
			.expect("Change must have either pre or post state");
		let op = change.op;
		self.authentication.push(change);
		self.log.push(Operation::Authentication {
			id,
			op,
		});
	}

	pub fn add_policy_change(&mut self, change: Change<Policy>) {
		let id = change
			.post
			.as_ref()
			.or(change.pre.as_ref())
			.map(|p| p.id)
			.expect("Change must have either pre or post state");
		let op = change.op;
		self.policy.push(change);
		self.log.push(Operation::Policy {
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
	Migration {
		id: MigrationId,
		op: OperationType,
	},
	MigrationEvent {
		id: MigrationEventId,
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
	Sink {
		id: SinkId,
		op: OperationType,
	},
	Source {
		id: SourceId,
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
	Test {
		id: TestId,
		op: OperationType,
	},
	Table {
		id: TableId,
		op: OperationType,
	},
	Identity {
		id: IdentityId,
		op: OperationType,
	},
	Authentication {
		id: AuthenticationId,
		op: OperationType,
	},
	Role {
		id: RoleId,
		op: OperationType,
	},
	GrantedRole {
		identity: IdentityId,
		role: RoleId,
		op: OperationType,
	},
	Policy {
		id: PolicyId,
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
			config_changes: Vec::new(),
			dictionary: Vec::new(),
			flow: Vec::new(),
			handler: Vec::new(),
			migration: Vec::new(),
			migration_event: Vec::new(),
			namespace: Vec::new(),
			procedure: Vec::new(),
			ringbuffer: Vec::new(),
			series: Vec::new(),
			sink: Vec::new(),
			source: Vec::new(),
			sumtype: Vec::new(),
			subscription: Vec::new(),
			test: Vec::new(),
			table: Vec::new(),
			identity: Vec::new(),
			authentication: Vec::new(),
			role: Vec::new(),
			granted_role: Vec::new(),
			policy: Vec::new(),
			view: Vec::new(),
			log: Vec::new(),
		}
	}

	/// Check if a table exists in this transaction's view
	pub fn table_exists(&self, id: TableId) -> bool {
		self.get_table(id).is_some()
	}

	/// Get current state of a table within this transaction
	pub fn get_table(&self, id: TableId) -> Option<&Table> {
		// Find the last change for this table ID
		for change in self.table.iter().rev() {
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
	pub fn view_exists(&self, id: ViewId) -> bool {
		self.get_view(id).is_some()
	}

	/// Get current state of a view within this transaction
	pub fn get_view(&self, id: ViewId) -> Option<&View> {
		// Find the last change for this view ID
		for change in self.view.iter().rev() {
			if let Some(view) = &change.post {
				if view.id() == id {
					return Some(view);
				}
			} else if let Some(view) = &change.pre {
				if view.id() == id && change.op == Delete {
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
	pub fn namespace(&self) -> &[Change<Namespace>] {
		&self.namespace
	}

	/// Get table definition changes
	pub fn table(&self) -> &[Change<Table>] {
		&self.table
	}

	/// Get view definition changes
	pub fn view(&self) -> &[Change<View>] {
		&self.view
	}

	/// Clear all changes (for rollback)
	pub fn clear(&mut self) {
		self.config_changes.clear();
		self.dictionary.clear();
		self.flow.clear();
		self.handler.clear();
		self.migration.clear();
		self.migration_event.clear();
		self.namespace.clear();
		self.procedure.clear();
		self.ringbuffer.clear();
		self.series.clear();
		self.sink.clear();
		self.source.clear();
		self.sumtype.clear();
		self.subscription.clear();
		self.test.clear();
		self.table.clear();
		self.identity.clear();
		self.authentication.clear();
		self.role.clear();
		self.granted_role.clear();
		self.policy.clear();
		self.view.clear();
		self.log.clear();
	}
}

/// Tracks a table row insertion for post-commit event emission
#[derive(Debug, Clone)]
pub struct TableRowInsertion {
	pub table_id: TableId,
	pub row_number: RowNumber,
	pub encoded: EncodedRow,
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
