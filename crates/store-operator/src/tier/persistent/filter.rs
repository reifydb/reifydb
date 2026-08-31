// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{interface::catalog::flow::OperatorId, key::operator::state::GroupId, util::bloom::hash_item};
use reifydb_store::filter::FilterDomain;
use reifydb_value::value::row_number::RowNumber;

pub const ARMED_CAPACITY_JOIN_EXPIRIES: u64 = 1_000_000;

pub struct JoinExpiryKeys;

impl FilterDomain for JoinExpiryKeys {
	type Key<'a> = (OperatorId, GroupId, u8, RowNumber);

	fn hash(key: Self::Key<'_>) -> u64 {
		hash_item(&(key.0.0, key.1.0, key.2, key.3.0))
	}
}
