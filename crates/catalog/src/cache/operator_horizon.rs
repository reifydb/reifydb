// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{common::CommitVersion, interface::catalog::flow::OperatorId, row::OperatorHorizon};

use crate::cache::{CatalogCache, MultiVersionOperatorHorizon};

impl CatalogCache {
	pub fn set_operator_horizon(
		&self,
		operator: OperatorId,
		version: CommitVersion,
		horizon: Option<OperatorHorizon>,
	) {
		let _guard = self.write_lock.lock();
		let multi = self.operator_horizon.get_or_insert_with(operator, MultiVersionOperatorHorizon::new);

		if let Some(new_horizon) = horizon {
			multi.value().insert(version, new_horizon);
		} else {
			multi.value().remove(version);
		}
	}
}
