// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::execute::Executor;
use crate::execute::sequence::SCHEMA_SEQUENCE_ID;
use reifydb_core::catalog::SchemaId;
use reifydb_core::row::EncodedRow;
use reifydb_core::{AsyncCowVec, EncodableKey, SequenceValueKey};
use reifydb_storage::VersionedStorage;
use reifydb_transaction::Tx;

impl<S: VersionedStorage> Executor<S> {
    pub(crate) fn next_schema_id(&mut self, tx: &mut impl Tx<S>) -> crate::Result<SchemaId> {
        // FIXME sequence exhausted
        // tx.set(Key::Schema(SchemaKey { schema_id: SchemaId(1) }).encode(), row)?;
        let key = SequenceValueKey { sequence_id: SCHEMA_SEQUENCE_ID }.encode();
        dbg!(&key);

        let result = match tx.get(&key)? {
            Some(value) => 1u32,
            None => 1u32,
        };

        tx.set(key, EncodedRow(AsyncCowVec::new(vec![])));

        todo!()
    }
}
