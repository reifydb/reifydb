// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::key::{EncodableKey, TableRowSequenceKey};
use crate::row::RowId;
use crate::sequence::u64::SequenceGeneratorU64;
use crate::table::TableId;
use reifydb_core::interface::{Tx, UnversionedStorage, VersionedStorage};

pub struct TableRowSequence {}

impl TableRowSequence {
    pub fn next_row_id<VS: VersionedStorage, US: UnversionedStorage>(
        tx: &mut impl Tx<VS, US>,
        table: TableId,
    ) -> crate::Result<RowId> {
        SequenceGeneratorU64::next(tx, &TableRowSequenceKey { table }.encode()).map(RowId)
    }
}
