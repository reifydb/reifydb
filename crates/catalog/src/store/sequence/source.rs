// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use once_cell::sync::Lazy;
use reifydb_core::{
	encoded::key::EncodedKey, interface::catalog::id::SourceId, key::system_sequence::SystemSequenceKey,
};
use reifydb_transaction::transaction::admin::AdminTransaction;

use crate::{Result, store::sequence::generator::u64::GeneratorU64, system::ids::sequences::SOURCE_CONNECTOR};

static SOURCE_CONNECTOR_KEY: Lazy<EncodedKey> = Lazy::new(|| SystemSequenceKey::encoded(SOURCE_CONNECTOR));

pub(crate) fn next_source_id(txn: &mut AdminTransaction) -> Result<SourceId> {
	GeneratorU64::next(txn, &SOURCE_CONNECTOR_KEY, None).map(SourceId)
}
