// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use once_cell::sync::Lazy;
use reifydb_core::{
	encoded::key::EncodedKey,
	interface::catalog::{
		authentication::AuthenticationId,
		id::{
			ColumnId, ColumnPropertyId, HandlerId, MigrationEventId, MigrationId, NamespaceId,
			PrimaryKeyId, ProcedureId, RingBufferId, SeriesId, TableId, TestId, ViewId,
		},
		identity::RoleId,
		policy::PolicyId,
		token::TokenId,
	},
	key::system_sequence::SystemSequenceKey,
};
use reifydb_transaction::transaction::admin::AdminTransaction;
use reifydb_type::value::{dictionary::DictionaryId, sumtype::SumTypeId};

use crate::{
	Result,
	store::sequence::generator::u64::GeneratorU64,
	system::ids::sequences::{
		AUTHENTICATION, COLUMN, COLUMN_PROPERTY, FLOW, FLOW_EDGE, FLOW_NODE, HANDLER, MIGRATION,
		MIGRATION_EVENT, NAMESPACE, POLICY, PRIMARY_KEY, PROCEDURE, ROLE, SOURCE, TEST, TOKEN,
	},
};

static NAMESPACE_KEY: Lazy<EncodedKey> = Lazy::new(|| SystemSequenceKey::encoded(NAMESPACE));

static SOURCE_KEY: Lazy<EncodedKey> = Lazy::new(|| SystemSequenceKey::encoded(SOURCE));

static COLUMN_KEY: Lazy<EncodedKey> = Lazy::new(|| SystemSequenceKey::encoded(COLUMN));

static COLUMN_PROPERTY_KEY: Lazy<EncodedKey> = Lazy::new(|| SystemSequenceKey::encoded(COLUMN_PROPERTY));

pub(crate) static FLOW_KEY: Lazy<EncodedKey> = Lazy::new(|| SystemSequenceKey::encoded(FLOW));

pub(crate) static FLOW_NODE_KEY: Lazy<EncodedKey> = Lazy::new(|| SystemSequenceKey::encoded(FLOW_NODE));

pub(crate) static FLOW_EDGE_KEY: Lazy<EncodedKey> = Lazy::new(|| SystemSequenceKey::encoded(FLOW_EDGE));

static PRIMARY_KEY_KEY: Lazy<EncodedKey> = Lazy::new(|| SystemSequenceKey::encoded(PRIMARY_KEY));

static PROCEDURE_KEY: Lazy<EncodedKey> = Lazy::new(|| SystemSequenceKey::encoded(PROCEDURE));

static HANDLER_KEY: Lazy<EncodedKey> = Lazy::new(|| SystemSequenceKey::encoded(HANDLER));

static ROLE_KEY: Lazy<EncodedKey> = Lazy::new(|| SystemSequenceKey::encoded(ROLE));

static POLICY_KEY: Lazy<EncodedKey> = Lazy::new(|| SystemSequenceKey::encoded(POLICY));

static MIGRATION_KEY: Lazy<EncodedKey> = Lazy::new(|| SystemSequenceKey::encoded(MIGRATION));

static MIGRATION_EVENT_KEY: Lazy<EncodedKey> = Lazy::new(|| SystemSequenceKey::encoded(MIGRATION_EVENT));

static AUTHENTICATION_KEY: Lazy<EncodedKey> = Lazy::new(|| SystemSequenceKey::encoded(AUTHENTICATION));

static TEST_KEY: Lazy<EncodedKey> = Lazy::new(|| SystemSequenceKey::encoded(TEST));

static TOKEN_KEY: Lazy<EncodedKey> = Lazy::new(|| SystemSequenceKey::encoded(TOKEN));

pub(crate) struct SystemSequence {}

/// IDs 1–16384 (2^14) are reserved for system objects across all sequences.
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
}
