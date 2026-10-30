// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::catalog::EncodedCatalogRow;
use reifydb_core::{
	interface::catalog::{
		flow::FlowId,
		id::{NamespaceId, WindowedViewGroupId},
		windowed_view_group::WindowedViewGroup,
	},
	key::catalog::WindowedViewGroupKey,
};
use reifydb_transaction::{multi::RangeScope, transaction::Transaction};

use crate::{
	CatalogStore, Result,
	store::windowed_view_group::shape::{deserialize_members, windowed_view_group},
};

impl CatalogStore {
	pub(crate) fn list_windowed_view_groups_all(rx: &mut Transaction<'_>) -> Result<Vec<WindowedViewGroup>> {
		let mut result = Vec::new();

		let stream = rx.range(WindowedViewGroupKey::full_scan(), RangeScope::All, 1024)?;

		for entry in stream {
			let entry = entry?;
			let bytes = EncodedCatalogRow::view(&entry.bytes);

			result.push(WindowedViewGroup {
				id: WindowedViewGroupId(windowed_view_group::get_id(bytes)),
				namespace: NamespaceId(windowed_view_group::get_namespace(bytes)),
				name: windowed_view_group::get_name(bytes).to_string(),
				flow: FlowId(windowed_view_group::get_flow(bytes)),
				members: deserialize_members(&windowed_view_group::get_members(bytes)),
			});
		}

		Ok(result)
	}
}
