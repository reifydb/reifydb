// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::sequence::generator::u64::GeneratorU64;
use once_cell::sync::Lazy;
pub use reifydb_core::interface::SystemSequenceId;
use reifydb_core::interface::{ColumnId, SchemaId};
use reifydb_core::{
	interface::{
		ActiveCommandTransaction, ColumnPolicyId, EncodableKey,
		SystemSequenceKey, TableId, Transaction,
	},
	EncodedKey,
};

pub(crate) const SCHEMA_SEQUENCE_ID: SystemSequenceId = SystemSequenceId(1);
pub(crate) const TABLE_SEQUENCE_ID: SystemSequenceId = SystemSequenceId(2);
pub(crate) const COLUMN_SEQUENCE_ID: SystemSequenceId = SystemSequenceId(3);
pub(crate) const COLUMN_POLICY_SEQUENCE_ID: SystemSequenceId =
	SystemSequenceId(4);

static SCHEMA_KEY: Lazy<EncodedKey> = Lazy::new(|| {
	SystemSequenceKey {
		sequence: SCHEMA_SEQUENCE_ID,
	}
	.encode()
});

static TABLE_KEY: Lazy<EncodedKey> = Lazy::new(|| {
	SystemSequenceKey {
		sequence: TABLE_SEQUENCE_ID,
	}
	.encode()
});

static COLUMN_KEY: Lazy<EncodedKey> = Lazy::new(|| {
	SystemSequenceKey {
		sequence: COLUMN_SEQUENCE_ID,
	}
	.encode()
});

static COLUMN_POLICY_KEY: Lazy<EncodedKey> = Lazy::new(|| {
	SystemSequenceKey {
		sequence: COLUMN_POLICY_SEQUENCE_ID,
	}
	.encode()
});

pub(crate) struct SystemSequence {}

impl SystemSequence {
	pub(crate) fn next_schema_id<T: Transaction>(
		txn: &mut ActiveCommandTransaction<T>,
	) -> crate::Result<SchemaId> {
		GeneratorU64::next(txn, &SCHEMA_KEY).map(SchemaId)
	}
}

impl SystemSequence {
	pub(crate) fn next_table_id<T: Transaction>(
		txn: &mut ActiveCommandTransaction<T>,
	) -> crate::Result<TableId> {
		GeneratorU64::next(txn, &TABLE_KEY).map(TableId)
	}
}

impl SystemSequence {
	pub(crate) fn next_column_id<T: Transaction>(
		txn: &mut ActiveCommandTransaction<T>,
	) -> crate::Result<ColumnId> {
		GeneratorU64::next(txn, &COLUMN_KEY).map(ColumnId)
	}
}

impl SystemSequence {
	pub(crate) fn next_column_policy_id<T: Transaction>(
		txn: &mut ActiveCommandTransaction<T>,
	) -> crate::Result<ColumnPolicyId> {
		GeneratorU64::next(txn, &COLUMN_POLICY_KEY).map(ColumnPolicyId)
	}
}
