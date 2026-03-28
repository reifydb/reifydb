// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use once_cell::sync::Lazy;
use reifydb_core::{encoded::key::EncodedKey, interface::catalog::id::SinkId, key::system_sequence::SystemSequenceKey};
use reifydb_transaction::transaction::admin::AdminTransaction;

use crate::{Result, store::sequence::generator::u64::GeneratorU64, system::ids::sequences::SINK_CONNECTOR};

static SINK_CONNECTOR_KEY: Lazy<EncodedKey> = Lazy::new(|| SystemSequenceKey::encoded(SINK_CONNECTOR));

pub(crate) fn next_sink_id(txn: &mut AdminTransaction) -> Result<SinkId> {
	GeneratorU64::next(txn, &SINK_CONNECTOR_KEY, None).map(SinkId)
}
