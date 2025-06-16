// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::Catalog;
use crate::column::ColumnId;
use crate::key::{EncodableKey, SequenceValueKey};
use crate::schema::SchemaId;
use crate::sequence::u32::SequenceGeneratorU32;
use crate::table::TableId;
use once_cell::sync::Lazy;
use reifydb_core::EncodedKey;
use reifydb_storage::{UnversionedStorage, VersionedStorage};
use reifydb_transaction::Tx;
use std::ops::Deref;

mod u32;

#[repr(transparent)]
#[derive(Debug, Copy, Clone, PartialOrd, PartialEq, Ord, Eq, Hash)]
pub struct SequenceId(pub u32);

impl Deref for SequenceId {
    type Target = u32;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl PartialEq<u32> for SequenceId {
    fn eq(&self, other: &u32) -> bool {
        self.0.eq(other)
    }
}

pub(crate) const SCHEMA_SEQUENCE_ID: SequenceId = SequenceId(1);
pub(crate) const TABLE_SEQUENCE_ID: SequenceId = SequenceId(2);
pub(crate) const COLUMN_SEQUENCE_ID: SequenceId = SequenceId(3);

static SCHEMA_KEY: Lazy<EncodedKey> =
    Lazy::new(|| SequenceValueKey { sequence_id: SCHEMA_SEQUENCE_ID }.encode());

static TABLE_KEY: Lazy<EncodedKey> =
    Lazy::new(|| SequenceValueKey { sequence_id: TABLE_SEQUENCE_ID }.encode());

static COLUMN_KEY: Lazy<EncodedKey> =
    Lazy::new(|| SequenceValueKey { sequence_id: COLUMN_SEQUENCE_ID }.encode());

impl Catalog {
    pub(crate) fn next_schema_id<VS: VersionedStorage, US: UnversionedStorage>(
        tx: &mut impl Tx<VS, US>,
    ) -> crate::Result<SchemaId> {
        SequenceGeneratorU32::next(tx, &SCHEMA_KEY).map(SchemaId)
    }
}

impl Catalog {
    pub(crate) fn next_table_id<VS: VersionedStorage, US: UnversionedStorage>(
        tx: &mut impl Tx<VS, US>,
    ) -> crate::Result<TableId> {
        SequenceGeneratorU32::next(tx, &TABLE_KEY).map(TableId)
    }
}

impl Catalog {
    pub(crate) fn next_column_id<VS: VersionedStorage, US: UnversionedStorage>(
        tx: &mut impl Tx<VS, US>,
    ) -> crate::Result<ColumnId> {
        SequenceGeneratorU32::next(tx, &COLUMN_KEY).map(ColumnId)
    }
}
