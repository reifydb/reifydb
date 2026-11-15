// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{interface::catalog::id::SourceId, key::system::SystemSequenceKey};
use reifydb_transaction::transaction::admin::AdminTransaction;
use reifydb_value::error::ValueError;

use crate::{store::sequence::generator::u64::GeneratorU64, system::ids::sequences::SOURCE_CONNECTOR};

const SOURCE_CONNECTOR_KEY: SystemSequenceKey = SystemSequenceKey {
	sequence: SOURCE_CONNECTOR,
};

pub(crate) fn next_object_id(txn: &mut AdminTransaction) -> Result<SourceId, ValueError> {
	GeneratorU64::next(txn, &SOURCE_CONNECTOR_KEY, None).map(SourceId)
}
