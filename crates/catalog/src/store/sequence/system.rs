// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use once_cell::sync::Lazy;
use reifydb_core::{
	EncodedKey,
	interface::{
		ColumnId, ColumnPolicyId, CommandTransaction, DictionaryId, NamespaceId, PrimaryKeyId, RingBufferId,
		SystemSequenceKey, TableId, ViewId,
	},
};

use crate::{
	store::sequence::generator::u64::GeneratorU64,
	system::ids::sequences::{COLUMN, COLUMN_POLICY, FLOW, FLOW_EDGE, FLOW_NODE, NAMESPACE, PRIMARY_KEY, SOURCE},
};

static NAMESPACE_KEY: Lazy<EncodedKey> = Lazy::new(|| SystemSequenceKey::encoded(NAMESPACE));

static SOURCE_KEY: Lazy<EncodedKey> = Lazy::new(|| SystemSequenceKey::encoded(SOURCE));

static COLUMN_KEY: Lazy<EncodedKey> = Lazy::new(|| SystemSequenceKey::encoded(COLUMN));

static COLUMN_POLICY_KEY: Lazy<EncodedKey> = Lazy::new(|| SystemSequenceKey::encoded(COLUMN_POLICY));

pub(crate) static FLOW_KEY: Lazy<EncodedKey> = Lazy::new(|| SystemSequenceKey::encoded(FLOW));

pub(crate) static FLOW_NODE_KEY: Lazy<EncodedKey> = Lazy::new(|| SystemSequenceKey::encoded(FLOW_NODE));

pub(crate) static FLOW_EDGE_KEY: Lazy<EncodedKey> = Lazy::new(|| SystemSequenceKey::encoded(FLOW_EDGE));

static PRIMARY_KEY_KEY: Lazy<EncodedKey> = Lazy::new(|| SystemSequenceKey::encoded(PRIMARY_KEY));

pub(crate) struct SystemSequence {}

impl SystemSequence {
	pub(crate) async fn next_namespace_id(txn: &mut impl CommandTransaction) -> crate::Result<NamespaceId> {
		GeneratorU64::next(txn, &NAMESPACE_KEY, Some(1025)).await.map(NamespaceId)
	}

	pub(crate) async fn next_table_id(txn: &mut impl CommandTransaction) -> crate::Result<TableId> {
		GeneratorU64::next(txn, &SOURCE_KEY, Some(1025)).await.map(TableId)
	}

	pub(crate) async fn next_column_id(txn: &mut impl CommandTransaction) -> crate::Result<ColumnId> {
		GeneratorU64::next(txn, &COLUMN_KEY, Some(8193)).await.map(ColumnId)
	}

	pub(crate) async fn next_column_policy_id(txn: &mut impl CommandTransaction) -> crate::Result<ColumnPolicyId> {
		GeneratorU64::next(txn, &COLUMN_POLICY_KEY, Some(1025)).await.map(ColumnPolicyId)
	}

	pub(crate) async fn next_view_id(txn: &mut impl CommandTransaction) -> crate::Result<ViewId> {
		GeneratorU64::next(txn, &SOURCE_KEY, Some(1025)).await.map(ViewId)
	}

	pub(crate) async fn next_primary_key_id(txn: &mut impl CommandTransaction) -> crate::Result<PrimaryKeyId> {
		GeneratorU64::next(txn, &PRIMARY_KEY_KEY, None).await.map(PrimaryKeyId)
	}

	pub(crate) async fn next_ringbuffer_id(txn: &mut impl CommandTransaction) -> crate::Result<RingBufferId> {
		GeneratorU64::next(txn, &SOURCE_KEY, Some(1025)).await.map(RingBufferId)
	}

	pub(crate) async fn next_dictionary_id(txn: &mut impl CommandTransaction) -> crate::Result<DictionaryId> {
		GeneratorU64::next(txn, &SOURCE_KEY, Some(1025)).await.map(DictionaryId)
	}
}
