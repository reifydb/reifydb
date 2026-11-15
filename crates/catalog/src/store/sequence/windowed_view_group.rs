// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{interface::catalog::id::WindowedViewGroupId, key::system::SystemSequenceKey};
use reifydb_transaction::transaction::admin::AdminTransaction;
use reifydb_value::error::ValueError;

use crate::{store::sequence::generator::u64::GeneratorU64, system::ids::sequences::WINDOWED_VIEW_GROUP};

const WINDOWED_VIEW_GROUP_KEY: SystemSequenceKey = SystemSequenceKey {
	sequence: WINDOWED_VIEW_GROUP,
};

pub(crate) fn next_windowed_view_group_id(txn: &mut AdminTransaction) -> Result<WindowedViewGroupId, ValueError> {
	GeneratorU64::next(txn, &WINDOWED_VIEW_GROUP_KEY, None).map(WindowedViewGroupId)
}
