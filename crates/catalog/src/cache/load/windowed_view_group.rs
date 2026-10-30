// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::catalog::EncodedCatalogRow;
use reifydb_core::{
	interface::{
		catalog::{
			flow::FlowId,
			id::{NamespaceId, WindowedViewGroupId},
			windowed_view_group::WindowedViewGroup,
		},
		store::MultiVersionRow,
	},
	key::catalog::WindowedViewGroupKey,
};
use reifydb_transaction::{multi::RangeScope, transaction::Transaction};

use super::CatalogCache;
use crate::{
	Result,
	store::windowed_view_group::shape::{deserialize_members, windowed_view_group},
};

pub(crate) fn load_windowed_view_groups(rx: &mut Transaction<'_>, catalog: &CatalogCache) -> Result<()> {
	let range = WindowedViewGroupKey::full_scan();
	let stream = rx.range(range, RangeScope::All, 1024)?;

	for entry in stream {
		let multi = entry?;
		let version = multi.version;
		let group = convert_windowed_view_group(multi)?;
		catalog.set_windowed_view_group(group.id, version, Some(group));
	}

	Ok(())
}

fn convert_windowed_view_group<K>(multi: MultiVersionRow<K>) -> Result<WindowedViewGroup> {
	let bytes = EncodedCatalogRow::try_from(multi.bytes)?;
	Ok(WindowedViewGroup {
		id: WindowedViewGroupId(windowed_view_group::get_id(&bytes)),
		namespace: NamespaceId(windowed_view_group::get_namespace(&bytes)),
		name: windowed_view_group::get_name(&bytes).to_string(),
		flow: FlowId(windowed_view_group::get_flow(&bytes)),
		members: deserialize_members(&windowed_view_group::get_members(&bytes)),
	})
}

#[cfg(test)]
mod tests {
	use reifydb_core::{common::CommitVersion, interface::catalog::id::ViewId};

	use super::*;
	use crate::store::windowed_view_group::shape::serialize_members;

	#[test]
	fn test_convert_keeps_member_declaration_order() {
		// A load that reorders members would rebuild the group with a different member list than was created.
		let members: Vec<ViewId> =
			[9, 3, 15, 1, 12, 7, 2, 14, 5, 11, 4, 13, 6, 10, 8].into_iter().map(ViewId).collect();
		let mut row = windowed_view_group::allocate();
		windowed_view_group::set_id(&mut row, 7);
		windowed_view_group::set_namespace(&mut row, 1);
		windowed_view_group::set_name(&mut row, "wide_group");
		windowed_view_group::set_flow(&mut row, 3);
		windowed_view_group::set_members(&mut row, &serialize_members(&members));

		let multi = MultiVersionRow {
			key: (),
			bytes: row.freeze().into(),
			version: CommitVersion(1),
		};

		let group = convert_windowed_view_group(multi).unwrap();
		assert_eq!(group.id, WindowedViewGroupId(7));
		assert_eq!(group.flow, FlowId(3));
		assert_eq!(group.members, members);
	}
}
