// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::execute::Executor;
use crate::execute::sequence::u32::SequenceGeneratorU32;
use once_cell::sync::Lazy;
use reifydb_core::catalog::{SchemaId, SequenceId, TableId};
use reifydb_core::{EncodableKey, EncodedKey, SequenceValueKey};
use reifydb_storage::{UnversionedStorage, VersionedStorage};
use reifydb_transaction::Tx;

mod u32;

pub(crate) const SCHEMA_SEQUENCE_ID: SequenceId = SequenceId(1);
pub(crate) const TABLE_SEQUENCE_ID: SequenceId = SequenceId(2);

static SCHEMA_KEY: Lazy<EncodedKey> =
    Lazy::new(|| SequenceValueKey { sequence_id: SCHEMA_SEQUENCE_ID }.encode());

static TABLE_KEY: Lazy<EncodedKey> =
    Lazy::new(|| SequenceValueKey { sequence_id: TABLE_SEQUENCE_ID }.encode());

impl<VS: VersionedStorage, US: UnversionedStorage> Executor<VS, US> {
    pub(crate) fn next_schema_id(&mut self, tx: &mut impl Tx<VS, US>) -> crate::Result<SchemaId> {
        SequenceGeneratorU32::next(tx, &SCHEMA_KEY).map(SchemaId)
    }
}

impl<VS: VersionedStorage, US: UnversionedStorage> Executor<VS, US> {
    pub(crate) fn next_table_id(&mut self, tx: &mut impl Tx<VS, US>) -> crate::Result<TableId> {
        SequenceGeneratorU32::next(tx, &TABLE_KEY).map(TableId)
    }
}
