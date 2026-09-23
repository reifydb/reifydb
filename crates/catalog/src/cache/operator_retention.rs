// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{common::CommitVersion, interface::catalog::flow::OperatorId, row::OperatorRetention};

use crate::cache::{CatalogCache, MultiVersionOperatorRetention};

impl CatalogCache {
	pub fn set_operator_retention(
		&self,
		operator: OperatorId,
		version: CommitVersion,
		retention: Option<OperatorRetention>,
	) {
		let _guard = self.write_lock.lock();
		let multi = self.operator_retention.get_or_insert_with(operator, MultiVersionOperatorRetention::new);

		if let Some(new_retention) = retention {
			multi.value().insert(version, new_retention);
		} else {
			multi.value().remove(version);
		}
	}
}
