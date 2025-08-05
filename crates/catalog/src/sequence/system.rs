// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::column::ColumnId;
use crate::column_policy::ColumnPolicyId;
use crate::schema::SchemaId;
use crate::sequence::generator::u64::GeneratorU64;
use once_cell::sync::Lazy;
use reifydb_core::EncodedKey;
use reifydb_core::interface::{
    ActiveWriteTransaction, EncodableKey, SystemSequenceKey, TableId, UnversionedTransaction,
    VersionedTransaction,
};

pub use reifydb_core::interface::SystemSequenceId;

pub(crate) const SCHEMA_SEQUENCE_ID: SystemSequenceId = SystemSequenceId(1);
pub(crate) const TABLE_SEQUENCE_ID: SystemSequenceId = SystemSequenceId(2);
pub(crate) const COLUMN_SEQUENCE_ID: SystemSequenceId = SystemSequenceId(3);
pub(crate) const COLUMN_POLICY_SEQUENCE_ID: SystemSequenceId = SystemSequenceId(4);

static SCHEMA_KEY: Lazy<EncodedKey> =
    Lazy::new(|| SystemSequenceKey { sequence: SCHEMA_SEQUENCE_ID }.encode());

static TABLE_KEY: Lazy<EncodedKey> =
    Lazy::new(|| SystemSequenceKey { sequence: TABLE_SEQUENCE_ID }.encode());

static COLUMN_KEY: Lazy<EncodedKey> =
    Lazy::new(|| SystemSequenceKey { sequence: COLUMN_SEQUENCE_ID }.encode());

static COLUMN_POLICY_KEY: Lazy<EncodedKey> =
    Lazy::new(|| SystemSequenceKey { sequence: COLUMN_POLICY_SEQUENCE_ID }.encode());

pub(crate) struct SystemSequence {}

impl SystemSequence {
    pub(crate) fn next_schema_id<VT: VersionedTransaction, UT: UnversionedTransaction>(
        atx: &mut ActiveWriteTransaction<VT, UT>,
    ) -> crate::Result<SchemaId> {
        GeneratorU64::next(atx, &SCHEMA_KEY).map(SchemaId)
    }
}

impl SystemSequence {
    pub(crate) fn next_table_id<VT: VersionedTransaction, UT: UnversionedTransaction>(
        atx: &mut ActiveWriteTransaction<VT, UT>,
    ) -> crate::Result<TableId> {
        GeneratorU64::next(atx, &TABLE_KEY).map(TableId)
    }
}

impl SystemSequence {
    pub(crate) fn next_column_id<VT: VersionedTransaction, UT: UnversionedTransaction>(
        atx: &mut ActiveWriteTransaction<VT, UT>,
    ) -> crate::Result<ColumnId> {
        GeneratorU64::next(atx, &COLUMN_KEY).map(ColumnId)
    }
}

impl SystemSequence {
    pub(crate) fn next_column_policy_id<VT: VersionedTransaction, UT: UnversionedTransaction>(
        atx: &mut ActiveWriteTransaction<VT, UT>,
    ) -> crate::Result<ColumnPolicyId> {
        GeneratorU64::next(atx, &COLUMN_POLICY_KEY).map(ColumnPolicyId)
    }
}
