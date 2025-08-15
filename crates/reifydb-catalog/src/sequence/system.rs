// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use once_cell::sync::Lazy;
pub use reifydb_core::interface::SystemSequenceId;
use reifydb_core::{
	EncodedKey,
	interface::{
		ActiveCommandTransaction, ColumnPolicyId, EncodableKey,
		SchemaId, SystemSequenceKey, TableColumnId, TableId,
		Transaction, ViewColumnId, ViewId,
	},
};

use crate::sequence::generator::u64::GeneratorU64;

pub(crate) const SCHEMA_SEQ_ID: SystemSequenceId = SystemSequenceId(1);
pub(crate) const TABLE_SEQ_ID: SystemSequenceId = SystemSequenceId(2);
pub(crate) const COLUMN_SEQ_ID: SystemSequenceId = SystemSequenceId(3);
pub(crate) const COLUMN_POLICY_SEQ_ID: SystemSequenceId = SystemSequenceId(4);
pub(crate) const VIEW_SEQ_ID: SystemSequenceId = SystemSequenceId(5);

static SCHEMA_KEY: Lazy<EncodedKey> = Lazy::new(|| {
	SystemSequenceKey {
		sequence: SCHEMA_SEQ_ID,
	}
	.encode()
});

static TABLE_KEY: Lazy<EncodedKey> = Lazy::new(|| {
	SystemSequenceKey {
		sequence: TABLE_SEQ_ID,
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

static VIEW_KEY: Lazy<EncodedKey> = Lazy::new(|| {
	SystemSequenceKey {
		sequence: VIEW_SEQ_ID,
	}
	.encode()
});

pub(crate) struct SystemSequence {}

impl SystemSequence {
	pub(crate) fn next_schema_id<T: Transaction>(
		txn: &mut ActiveCommandTransaction<T>,
	) -> crate::Result<SchemaId> {
		GeneratorU64::next(txn, &SCHEMA_KEY, Some(1025)).map(SchemaId)
	}

	pub(crate) fn next_table_id<T: Transaction>(
		txn: &mut ActiveCommandTransaction<T>,
	) -> crate::Result<TableId> {
		GeneratorU64::next(txn, &TABLE_KEY, Some(1025)).map(TableId)
	}

	pub(crate) fn next_column_id<T: Transaction>(
		txn: &mut ActiveCommandTransaction<T>,
	) -> crate::Result<TableColumnId> {
		GeneratorU64::next(txn, &COLUMN_KEY, None).map(TableColumnId)
	}

	pub(crate) fn next_view_column_id<T: Transaction>(
		txn: &mut ActiveCommandTransaction<T>,
	) -> crate::Result<ViewColumnId> {
		GeneratorU64::next(txn, &COLUMN_KEY, None).map(ViewColumnId)
	}

	pub(crate) fn next_column_policy_id<T: Transaction>(
		txn: &mut ActiveCommandTransaction<T>,
	) -> crate::Result<ColumnPolicyId> {
		GeneratorU64::next(txn, &COLUMN_POLICY_KEY, None)
			.map(ColumnPolicyId)
	}

	pub(crate) fn next_view_id<T: Transaction>(
		txn: &mut ActiveCommandTransaction<T>,
	) -> crate::Result<ViewId> {
		GeneratorU64::next(txn, &VIEW_KEY, Some(1025)).map(ViewId)
	}
}
