// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	interface::catalog::{
		authentication::AuthenticationId,
		authentication_method::AuthenticationMethodId,
		id::{
			BindingId, ColumnBlockId, ColumnId, ColumnPropertyId, HandlerId, MigrationEventId, MigrationId,
			NamespaceId, PrimaryKeyId, ProcedureId, QueueId, RelationshipId, RingBufferId, SeriesId,
			TableId, TestId, ViewId,
		},
		identity::{IdentityAttributeId, RoleId},
		policy::PolicyId,
		token::TokenId,
	},
	key::system::SystemSequenceKey,
};
use reifydb_transaction::transaction::admin::AdminTransaction;
use reifydb_value::{
	error::ValueError,
	value::{dictionary::DictionaryId, sumtype::SumTypeId},
};

use crate::{
	store::sequence::generator::u64::GeneratorU64,
	system::ids::sequences::{
		AUTHENTICATION, AUTHENTICATION_METHOD, BINDING, COLUMN, COLUMN_BLOCK, COLUMN_PROPERTY, FLOW, FLOW_EDGE,
		HANDLER, IDENTITY_ATTRIBUTE, MIGRATION, MIGRATION_EVENT, NAMESPACE, OPERATOR, POLICY, PRIMARY_KEY,
		PROCEDURE, RELATIONSHIP, ROLE, SOURCE, TEST, TOKEN,
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

const AUTHENTICATION_METHOD_KEY: SystemSequenceKey = SystemSequenceKey {
	sequence: AUTHENTICATION_METHOD,
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

const COLUMN_BLOCK_KEY: SystemSequenceKey = SystemSequenceKey {
	sequence: COLUMN_BLOCK,
};
const RELATIONSHIP_KEY: SystemSequenceKey = SystemSequenceKey {
	sequence: RELATIONSHIP,
};

pub(crate) struct SystemSequence {}

const SYSTEM_RESERVED: u64 = 16385;

impl SystemSequence {
	pub(crate) fn next_namespace_id(txn: &mut AdminTransaction) -> Result<NamespaceId, ValueError> {
		GeneratorU64::next(txn, &NAMESPACE_KEY, Some(SYSTEM_RESERVED)).map(NamespaceId)
	}

	pub(crate) fn next_table_id(txn: &mut AdminTransaction) -> Result<TableId, ValueError> {
		GeneratorU64::next(txn, &SOURCE_KEY, Some(SYSTEM_RESERVED)).map(TableId)
	}

	pub(crate) fn next_column_id(txn: &mut AdminTransaction) -> Result<ColumnId, ValueError> {
		GeneratorU64::next(txn, &COLUMN_KEY, Some(SYSTEM_RESERVED)).map(ColumnId)
	}

	pub(crate) fn next_column_property_id(txn: &mut AdminTransaction) -> Result<ColumnPropertyId, ValueError> {
		GeneratorU64::next(txn, &COLUMN_PROPERTY_KEY, Some(SYSTEM_RESERVED)).map(ColumnPropertyId)
	}

	pub(crate) fn next_view_id(txn: &mut AdminTransaction) -> Result<ViewId, ValueError> {
		GeneratorU64::next(txn, &SOURCE_KEY, Some(SYSTEM_RESERVED)).map(ViewId)
	}

	pub(crate) fn next_primary_key_id(txn: &mut AdminTransaction) -> Result<PrimaryKeyId, ValueError> {
		GeneratorU64::next(txn, &PRIMARY_KEY_KEY, Some(SYSTEM_RESERVED)).map(PrimaryKeyId)
	}

	pub(crate) fn next_ringbuffer_id(txn: &mut AdminTransaction) -> Result<RingBufferId, ValueError> {
		GeneratorU64::next(txn, &SOURCE_KEY, Some(SYSTEM_RESERVED)).map(RingBufferId)
	}

	pub(crate) fn next_queue_id(txn: &mut AdminTransaction) -> Result<QueueId, ValueError> {
		GeneratorU64::next(txn, &SOURCE_KEY, Some(SYSTEM_RESERVED)).map(QueueId)
	}

	pub(crate) fn next_dictionary_id(txn: &mut AdminTransaction) -> Result<DictionaryId, ValueError> {
		GeneratorU64::next(txn, &SOURCE_KEY, Some(SYSTEM_RESERVED)).map(DictionaryId)
	}

	pub(crate) fn next_sumtype_id(txn: &mut AdminTransaction) -> Result<SumTypeId, ValueError> {
		GeneratorU64::next(txn, &SOURCE_KEY, Some(SYSTEM_RESERVED)).map(SumTypeId)
	}

	pub(crate) fn next_procedure_id(txn: &mut AdminTransaction) -> Result<ProcedureId, ValueError> {
		GeneratorU64::next(txn, &PROCEDURE_KEY, Some(SYSTEM_RESERVED)).map(ProcedureId::persistent)
	}

	pub(crate) fn next_handler_id(txn: &mut AdminTransaction) -> Result<HandlerId, ValueError> {
		GeneratorU64::next(txn, &HANDLER_KEY, Some(SYSTEM_RESERVED)).map(HandlerId)
	}

	pub(crate) fn next_series_id(txn: &mut AdminTransaction) -> Result<SeriesId, ValueError> {
		GeneratorU64::next(txn, &SOURCE_KEY, Some(SYSTEM_RESERVED)).map(SeriesId)
	}

	pub(crate) fn next_role_id(txn: &mut AdminTransaction) -> Result<RoleId, ValueError> {
		GeneratorU64::next(txn, &ROLE_KEY, Some(SYSTEM_RESERVED))
	}

	pub(crate) fn next_identity_attribute_id(
		txn: &mut AdminTransaction,
	) -> Result<IdentityAttributeId, ValueError> {
		GeneratorU64::next(txn, &IDENTITY_ATTRIBUTE_KEY, Some(SYSTEM_RESERVED))
	}

	pub(crate) fn next_policy_id(txn: &mut AdminTransaction) -> Result<PolicyId, ValueError> {
		GeneratorU64::next(txn, &POLICY_KEY, Some(SYSTEM_RESERVED))
	}

	pub(crate) fn next_migration_id(txn: &mut AdminTransaction) -> Result<MigrationId, ValueError> {
		GeneratorU64::next(txn, &MIGRATION_KEY, Some(SYSTEM_RESERVED)).map(MigrationId)
	}

	pub(crate) fn next_migration_event_id(txn: &mut AdminTransaction) -> Result<MigrationEventId, ValueError> {
		GeneratorU64::next(txn, &MIGRATION_EVENT_KEY, Some(SYSTEM_RESERVED)).map(MigrationEventId)
	}

	pub(crate) fn next_authentication_id(txn: &mut AdminTransaction) -> Result<AuthenticationId, ValueError> {
		GeneratorU64::next(txn, &AUTHENTICATION_KEY, Some(SYSTEM_RESERVED))
	}

	pub(crate) fn next_authentication_method_id(
		txn: &mut AdminTransaction,
	) -> Result<AuthenticationMethodId, ValueError> {
		GeneratorU64::next(txn, &AUTHENTICATION_METHOD_KEY, Some(SYSTEM_RESERVED))
	}

	pub(crate) fn next_test_id(txn: &mut AdminTransaction) -> Result<TestId, ValueError> {
		GeneratorU64::next(txn, &TEST_KEY, Some(SYSTEM_RESERVED)).map(TestId)
	}

	pub(crate) fn next_token_id(txn: &mut AdminTransaction) -> Result<TokenId, ValueError> {
		GeneratorU64::next(txn, &TOKEN_KEY, Some(SYSTEM_RESERVED))
	}

	pub(crate) fn next_binding_id(txn: &mut AdminTransaction) -> Result<BindingId, ValueError> {
		GeneratorU64::next(txn, &BINDING_KEY, Some(SYSTEM_RESERVED)).map(BindingId)
	}

	pub(crate) fn next_column_block_id(txn: &mut AdminTransaction) -> Result<ColumnBlockId, ValueError> {
		GeneratorU64::next(txn, &COLUMN_BLOCK_KEY, Some(SYSTEM_RESERVED)).map(ColumnBlockId)
	}

	pub(crate) fn next_relationship_id(txn: &mut AdminTransaction) -> Result<RelationshipId, ValueError> {
		GeneratorU64::next(txn, &RELATIONSHIP_KEY, Some(SYSTEM_RESERVED)).map(RelationshipId)
	}
}
