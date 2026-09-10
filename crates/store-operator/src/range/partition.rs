// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{interface::catalog::flow::OperatorId, key::operator::state::GroupId};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct TypedPartition {
	pub operator: OperatorId,
	pub group: GroupId,
}
