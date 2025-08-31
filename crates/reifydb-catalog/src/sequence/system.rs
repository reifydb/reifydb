// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use once_cell::sync::Lazy;
use reifydb_core::{
	EncodedKey,
	interface::{
		ColumnId, ColumnPolicyId, CommandTransaction, EncodableKey,
		SchemaId, SystemSequenceKey, TableId, ViewId,
	},
};

use crate::{
	sequence::generator::u64::GeneratorU64,
	system::ids::sequences::{
		COLUMN, COLUMN_POLICY, FLOW, FLOW_EDGE, FLOW_NODE, SCHEMA,
		STORE,
	},
};

static SCHEMA_KEY: Lazy<EncodedKey> = Lazy::new(|| {
	SystemSequenceKey {
		sequence: SCHEMA,
	}
	.encode()
});

static STORE_KEY: Lazy<EncodedKey> = Lazy::new(|| {
	SystemSequenceKey {
		sequence: STORE,
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

pub(crate) struct SystemSequence {}

impl SystemSequence {
	pub(crate) fn next_schema_id(
		txn: &mut impl CommandTransaction,
	) -> crate::Result<SchemaId> {
		GeneratorU64::next(txn, &SCHEMA_KEY, Some(1025)).map(SchemaId)
	}

	pub(crate) fn next_table_id(
		txn: &mut impl CommandTransaction,
	) -> crate::Result<TableId> {
		GeneratorU64::next(txn, &STORE_KEY, Some(1025)).map(TableId)
	}

	pub(crate) fn next_column_id(
		txn: &mut impl CommandTransaction,
	) -> crate::Result<ColumnId> {
		GeneratorU64::next(txn, &COLUMN_KEY, Some(8193)).map(ColumnId)
	}

	pub(crate) fn next_column_policy_id(
		txn: &mut impl CommandTransaction,
	) -> crate::Result<ColumnPolicyId> {
		GeneratorU64::next(txn, &COLUMN_POLICY_KEY, Some(1025))
			.map(ColumnPolicyId)
	}

	pub(crate) fn next_view_id(
		txn: &mut impl CommandTransaction,
	) -> crate::Result<ViewId> {
		GeneratorU64::next(txn, &STORE_KEY, Some(1025)).map(ViewId)
	}
}
