// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{interface::catalog::id::SinkId, key::system::SystemSequenceKey};
use reifydb_transaction::transaction::admin::AdminTransaction;

use crate::{Result, store::sequence::generator::u64::GeneratorU64, system::ids::sequences::SINK_CONNECTOR};

const SINK_CONNECTOR_KEY: SystemSequenceKey = SystemSequenceKey {
	sequence: SINK_CONNECTOR,
};

pub(crate) fn next_sink_id(txn: &mut AdminTransaction) -> Result<SinkId> {
	GeneratorU64::next(txn, &SINK_CONNECTOR_KEY, None).map(SinkId)
}
