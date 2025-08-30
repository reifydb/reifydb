// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use once_cell::sync::Lazy;
pub use reifydb_core::interface::SequenceId;
use reifydb_core::{
	EncodedKey,
	interface::{
		ColumnId, ColumnPolicyId, CommandTransaction, EncodableKey,
		SchemaId, SystemSequenceKey, TableId, ViewId,
	},
};

use crate::sequence::generator::u64::GeneratorU64;

pub(crate) const SCHEMA_SEQ_ID: SequenceId = SequenceId(1);
pub(crate) const STORE_SEQ_ID: SequenceId = SequenceId(2);
pub(crate) const COLUMN_SEQ_ID: SequenceId = SequenceId(3);
pub(crate) const COLUMN_POLICY_SEQ_ID: SequenceId = SequenceId(4);
pub(crate) const FLOW_SEQ_ID: SequenceId = SequenceId(5);
pub(crate) const FLOW_NODE_SEQ_ID: SequenceId = SequenceId(6);
pub(crate) const FLOW_EDGE_SEQ_ID: SequenceId = SequenceId(7);

pub(crate) const ALL_SYSTEM_SEQUENCE_IDS: [SequenceId; 7] = [
	SCHEMA_SEQ_ID,
	STORE_SEQ_ID,
	COLUMN_SEQ_ID,
	COLUMN_POLICY_SEQ_ID,
	FLOW_SEQ_ID,
	FLOW_NODE_SEQ_ID,
	FLOW_EDGE_SEQ_ID,
];

static SCHEMA_KEY: Lazy<EncodedKey> = Lazy::new(|| {
	SystemSequenceKey {
		sequence: SCHEMA_SEQ_ID,
	}
	.encode()
});

static STORE_KEY: Lazy<EncodedKey> = Lazy::new(|| {
	SystemSequenceKey {
		sequence: STORE_SEQ_ID,
	}
	.encode()
});

static COLUMN_KEY: Lazy<EncodedKey> = Lazy::new(|| {
	SystemSequenceKey {
		sequence: COLUMN_SEQ_ID,
	}
	.encode()
});

static COLUMN_POLICY_KEY: Lazy<EncodedKey> = Lazy::new(|| {
	SystemSequenceKey {
		sequence: COLUMN_POLICY_SEQ_ID,
	}
	.encode()
});

pub(crate) static FLOW_KEY: Lazy<EncodedKey> = Lazy::new(|| {
	SystemSequenceKey {
		sequence: FLOW_SEQ_ID,
	}
	.encode()
});

pub(crate) static FLOW_NODE_KEY: Lazy<EncodedKey> = Lazy::new(|| {
	SystemSequenceKey {
		sequence: FLOW_NODE_SEQ_ID,
	}
	.encode()
});

pub(crate) static FLOW_EDGE_KEY: Lazy<EncodedKey> = Lazy::new(|| {
	SystemSequenceKey {
		sequence: FLOW_EDGE_SEQ_ID,
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
		GeneratorU64::next(txn, &COLUMN_KEY, None).map(ColumnId)
	}

	pub(crate) fn next_column_policy_id(
		txn: &mut impl CommandTransaction,
	) -> crate::Result<ColumnPolicyId> {
		GeneratorU64::next(txn, &COLUMN_POLICY_KEY, None)
			.map(ColumnPolicyId)
	}

	pub(crate) fn next_view_id(
		txn: &mut impl CommandTransaction,
	) -> crate::Result<ViewId> {
		GeneratorU64::next(txn, &STORE_KEY, Some(1025)).map(ViewId)
	}
}
