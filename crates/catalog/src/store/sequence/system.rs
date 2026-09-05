// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	interface::catalog::{
		authentication::AuthenticationId,
		id::{
			BindingId, ColumnId, ColumnPropertyId, ColumnSnapshotId, HandlerId, MigrationEventId,
			MigrationId, NamespaceId, PrimaryKeyId, ProcedureId, QueueId, RelationshipId, RingBufferId,
			SeriesId, TableId, TestId, ViewId,
		},
		identity::{IdentityAttributeId, RoleId},
		policy::PolicyId,
		token::TokenId,
	},
	key::system::SystemSequenceKey,
};
use reifydb_transaction::transaction::admin::AdminTransaction;
use reifydb_value::value::{dictionary::DictionaryId, sumtype::SumTypeId};

use crate::{
	Result,
	store::sequence::generator::u64::GeneratorU64,
	system::ids::sequences::{
		AUTHENTICATION, BINDING, COLUMN, COLUMN_PROPERTY, COLUMN_SNAPSHOT, FLOW, FLOW_EDGE, HANDLER,
		IDENTITY_ATTRIBUTE, MIGRATION, MIGRATION_EVENT, NAMESPACE, OPERATOR, POLICY, PRIMARY_KEY, PROCEDURE,
		RELATIONSHIP, ROLE, SOURCE, TEST, TOKEN,
	},
};

const NAMESPACE_KEY: SystemSequenceKey = SystemSequenceKey {
	sequence: NAMESPACE,
};

const SOURCE_KEY: SystemSequenceKey = SystemSequenceKey {
	sequence: SOURCE,
};

const COLUMN_KEY: SystemSequenceKey = SystemSequenceKey {
	sequence: COLUMN,
};

const COLUMN_PROPERTY_KEY: SystemSequenceKey = SystemSequenceKey {
	sequence: COLUMN_PROPERTY,
};

pub(crate) const FLOW_KEY: SystemSequenceKey = SystemSequenceKey {
	sequence: FLOW,
};

pub(crate) const FLOW_NODE_KEY: SystemSequenceKey = SystemSequenceKey {
	sequence: OPERATOR,
};

pub(crate) const FLOW_EDGE_KEY: SystemSequenceKey = SystemSequenceKey {
	sequence: FLOW_EDGE,
};

const PRIMARY_KEY_KEY: SystemSequenceKey = SystemSequenceKey {
	sequence: PRIMARY_KEY,
};

const PROCEDURE_KEY: SystemSequenceKey = SystemSequenceKey {
	sequence: PROCEDURE,
};

const HANDLER_KEY: SystemSequenceKey = SystemSequenceKey {
	sequence: HANDLER,
};

const ROLE_KEY: SystemSequenceKey = SystemSequenceKey {
	sequence: ROLE,
};

const IDENTITY_ATTRIBUTE_KEY: SystemSequenceKey = SystemSequenceKey {
	sequence: IDENTITY_ATTRIBUTE,
};

const POLICY_KEY: SystemSequenceKey = SystemSequenceKey {
	sequence: POLICY,
};

const MIGRATION_KEY: SystemSequenceKey = SystemSequenceKey {
	sequence: MIGRATION,
};

const MIGRATION_EVENT_KEY: SystemSequenceKey = SystemSequenceKey {
	sequence: MIGRATION_EVENT,
};

const AUTHENTICATION_KEY: SystemSequenceKey = SystemSequenceKey {
	sequence: AUTHENTICATION,
};

const TEST_KEY: SystemSequenceKey = SystemSequenceKey {
	sequence: TEST,
};

const TOKEN_KEY: SystemSequenceKey = SystemSequenceKey {
	sequence: TOKEN,
};

const BINDING_KEY: SystemSequenceKey = SystemSequenceKey {
	sequence: BINDING,
};

const COLUMN_SNAPSHOT_KEY: SystemSequenceKey = SystemSequenceKey {
	sequence: COLUMN_SNAPSHOT,
};
const RELATIONSHIP_KEY: SystemSequenceKey = SystemSequenceKey {
	sequence: RELATIONSHIP,
};

pub(crate) struct SystemSequence {}

const SYSTEM_RESERVED: u64 = 16385;

impl SystemSequence {
	pub(crate) fn next_namespace_id(txn: &mut AdminTransaction) -> Result<NamespaceId> {
		GeneratorU64::next(txn, &NAMESPACE_KEY, Some(SYSTEM_RESERVED)).map(NamespaceId)
	}

	pub(crate) fn next_table_id(txn: &mut AdminTransaction) -> Result<TableId> {
		GeneratorU64::next(txn, &SOURCE_KEY, Some(SYSTEM_RESERVED)).map(TableId)
	}

	pub(crate) fn next_column_id(txn: &mut AdminTransaction) -> Result<ColumnId> {
		GeneratorU64::next(txn, &COLUMN_KEY, Some(SYSTEM_RESERVED)).map(ColumnId)
	}

	pub(crate) fn next_column_property_id(txn: &mut AdminTransaction) -> Result<ColumnPropertyId> {
		GeneratorU64::next(txn, &COLUMN_PROPERTY_KEY, Some(SYSTEM_RESERVED)).map(ColumnPropertyId)
	}

	pub(crate) fn next_view_id(txn: &mut AdminTransaction) -> Result<ViewId> {
		GeneratorU64::next(txn, &SOURCE_KEY, Some(SYSTEM_RESERVED)).map(ViewId)
	}

	pub(crate) fn next_primary_key_id(txn: &mut AdminTransaction) -> Result<PrimaryKeyId> {
		GeneratorU64::next(txn, &PRIMARY_KEY_KEY, Some(SYSTEM_RESERVED)).map(PrimaryKeyId)
	}

	pub(crate) fn next_ringbuffer_id(txn: &mut AdminTransaction) -> Result<RingBufferId> {
		GeneratorU64::next(txn, &SOURCE_KEY, Some(SYSTEM_RESERVED)).map(RingBufferId)
	}

	pub(crate) fn next_queue_id(txn: &mut AdminTransaction) -> Result<QueueId> {
		GeneratorU64::next(txn, &SOURCE_KEY, Some(SYSTEM_RESERVED)).map(QueueId)
	}

	pub(crate) fn next_dictionary_id(txn: &mut AdminTransaction) -> Result<DictionaryId> {
		GeneratorU64::next(txn, &SOURCE_KEY, Some(SYSTEM_RESERVED)).map(DictionaryId)
	}

	pub(crate) fn next_sumtype_id(txn: &mut AdminTransaction) -> Result<SumTypeId> {
		GeneratorU64::next(txn, &SOURCE_KEY, Some(SYSTEM_RESERVED)).map(SumTypeId)
	}

	pub(crate) fn next_procedure_id(txn: &mut AdminTransaction) -> Result<ProcedureId> {
		GeneratorU64::next(txn, &PROCEDURE_KEY, Some(SYSTEM_RESERVED)).map(ProcedureId::persistent)
	}

	pub(crate) fn next_handler_id(txn: &mut AdminTransaction) -> Result<HandlerId> {
		GeneratorU64::next(txn, &HANDLER_KEY, Some(SYSTEM_RESERVED)).map(HandlerId)
	}

	pub(crate) fn next_series_id(txn: &mut AdminTransaction) -> Result<SeriesId> {
		GeneratorU64::next(txn, &SOURCE_KEY, Some(SYSTEM_RESERVED)).map(SeriesId)
	}

	pub(crate) fn next_role_id(txn: &mut AdminTransaction) -> Result<RoleId> {
		GeneratorU64::next(txn, &ROLE_KEY, Some(SYSTEM_RESERVED))
	}

	pub(crate) fn next_identity_attribute_id(txn: &mut AdminTransaction) -> Result<IdentityAttributeId> {
		GeneratorU64::next(txn, &IDENTITY_ATTRIBUTE_KEY, Some(SYSTEM_RESERVED))
	}

	pub(crate) fn next_policy_id(txn: &mut AdminTransaction) -> Result<PolicyId> {
		GeneratorU64::next(txn, &POLICY_KEY, Some(SYSTEM_RESERVED))
	}

	pub(crate) fn next_migration_id(txn: &mut AdminTransaction) -> Result<MigrationId> {
		GeneratorU64::next(txn, &MIGRATION_KEY, Some(SYSTEM_RESERVED)).map(MigrationId)
	}

	pub(crate) fn next_migration_event_id(txn: &mut AdminTransaction) -> Result<MigrationEventId> {
		GeneratorU64::next(txn, &MIGRATION_EVENT_KEY, Some(SYSTEM_RESERVED)).map(MigrationEventId)
	}

	pub(crate) fn next_authentication_id(txn: &mut AdminTransaction) -> Result<AuthenticationId> {
		GeneratorU64::next(txn, &AUTHENTICATION_KEY, Some(SYSTEM_RESERVED))
	}

	pub(crate) fn next_test_id(txn: &mut AdminTransaction) -> Result<TestId> {
		GeneratorU64::next(txn, &TEST_KEY, Some(SYSTEM_RESERVED)).map(TestId)
	}

	pub(crate) fn next_token_id(txn: &mut AdminTransaction) -> Result<TokenId> {
		GeneratorU64::next(txn, &TOKEN_KEY, Some(SYSTEM_RESERVED))
	}

	pub(crate) fn next_binding_id(txn: &mut AdminTransaction) -> Result<BindingId> {
		GeneratorU64::next(txn, &BINDING_KEY, Some(SYSTEM_RESERVED)).map(BindingId)
	}

	pub(crate) fn next_column_snapshot_id(txn: &mut AdminTransaction) -> Result<ColumnSnapshotId> {
		GeneratorU64::next(txn, &COLUMN_SNAPSHOT_KEY, Some(SYSTEM_RESERVED)).map(ColumnSnapshotId)
	}

	pub(crate) fn next_relationship_id(txn: &mut AdminTransaction) -> Result<RelationshipId> {
		GeneratorU64::next(txn, &RELATIONSHIP_KEY, Some(SYSTEM_RESERVED)).map(RelationshipId)
	}
}
