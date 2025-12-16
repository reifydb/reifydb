// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use once_cell::sync::Lazy;
use reifydb_core::{
	EncodedKey,
	interface::{
		ColumnId, ColumnPolicyId, CommandTransaction, DictionaryId, EncodableKey, NamespaceId, PrimaryKeyId,
		RingBufferId, SystemSequenceKey, TableId, ViewId,
	},
};

use crate::{
	store::sequence::generator::u64::GeneratorU64,
	system::ids::sequences::{COLUMN, COLUMN_POLICY, FLOW, FLOW_EDGE, FLOW_NODE, NAMESPACE, PRIMARY_KEY, SOURCE},
};

static NAMESPACE_KEY: Lazy<EncodedKey> = Lazy::new(|| {
	SystemSequenceKey {
		sequence: NAMESPACE,
	}
	.encode()
});

static SOURCE_KEY: Lazy<EncodedKey> = Lazy::new(|| {
	SystemSequenceKey {
		sequence: SOURCE,
	}
	.encode()
});

static COLUMN_KEY: Lazy<EncodedKey> = Lazy::new(|| {
	SystemSequenceKey {
		sequence: COLUMN,
	}
	.encode()
});

static COLUMN_POLICY_KEY: Lazy<EncodedKey> = Lazy::new(|| {
	SystemSequenceKey {
		sequence: COLUMN_POLICY,
	}
	.encode()
});

pub(crate) static FLOW_KEY: Lazy<EncodedKey> = Lazy::new(|| {
	SystemSequenceKey {
		sequence: FLOW,
	}
	.encode()
});

pub(crate) static FLOW_NODE_KEY: Lazy<EncodedKey> = Lazy::new(|| {
	SystemSequenceKey {
		sequence: FLOW_NODE,
	}
	.encode()
});

pub(crate) static FLOW_EDGE_KEY: Lazy<EncodedKey> = Lazy::new(|| {
	SystemSequenceKey {
		sequence: FLOW_EDGE,
	}
	.encode()
});

static PRIMARY_KEY_KEY: Lazy<EncodedKey> = Lazy::new(|| {
	SystemSequenceKey {
		sequence: PRIMARY_KEY,
	}
	.encode()
});

pub(crate) struct SystemSequence {}

impl SystemSequence {
	pub(crate) fn next_namespace_id(txn: &mut impl CommandTransaction) -> crate::Result<NamespaceId> {
		GeneratorU64::next(txn, &NAMESPACE_KEY, Some(1025)).map(NamespaceId)
	}

	pub(crate) fn next_table_id(txn: &mut impl CommandTransaction) -> crate::Result<TableId> {
		GeneratorU64::next(txn, &SOURCE_KEY, Some(1025)).map(TableId)
	}

	pub(crate) fn next_column_id(txn: &mut impl CommandTransaction) -> crate::Result<ColumnId> {
		GeneratorU64::next(txn, &COLUMN_KEY, Some(8193)).map(ColumnId)
	}

	pub(crate) fn next_column_policy_id(txn: &mut impl CommandTransaction) -> crate::Result<ColumnPolicyId> {
		GeneratorU64::next(txn, &COLUMN_POLICY_KEY, Some(1025)).map(ColumnPolicyId)
	}

	pub(crate) fn next_view_id(txn: &mut impl CommandTransaction) -> crate::Result<ViewId> {
		GeneratorU64::next(txn, &SOURCE_KEY, Some(1025)).map(ViewId)
	}

	pub(crate) fn next_primary_key_id(txn: &mut impl CommandTransaction) -> crate::Result<PrimaryKeyId> {
		GeneratorU64::next(txn, &PRIMARY_KEY_KEY, None).map(PrimaryKeyId)
	}

	pub(crate) fn next_ringbuffer_id(txn: &mut impl CommandTransaction) -> crate::Result<RingBufferId> {
		GeneratorU64::next(txn, &SOURCE_KEY, Some(1025)).map(RingBufferId)
	}

	pub(crate) fn next_dictionary_id(txn: &mut impl CommandTransaction) -> crate::Result<DictionaryId> {
		GeneratorU64::next(txn, &SOURCE_KEY, Some(1025)).map(DictionaryId)
	}
}
