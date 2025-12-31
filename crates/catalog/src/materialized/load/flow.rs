// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::{FlowDef, FlowId, FlowKey, FlowStatus, MultiVersionValues, NamespaceId};
use reifydb_transaction::IntoStandardTransaction;

use crate::{
	MaterializedCatalog,
	store::flow::layout::{
		flow,
		flow::{ID, NAME, NAMESPACE, STATUS},
	},
};

pub(crate) async fn load_flows(
	rx: &mut impl IntoStandardTransaction,
	catalog: &MaterializedCatalog,
) -> crate::Result<()> {
	let mut txn = rx.into_standard_transaction();
	let range = FlowKey::full_scan();
	let batch = txn.range_batch(range, 1024).await?;

	for multi in batch.items {
		let version = multi.version;
		let flow_def = convert_flow(multi);
		catalog.set_flow(flow_def.id, version, Some(flow_def));
	}

	Ok(())
}

fn convert_flow(multi: MultiVersionValues) -> FlowDef {
	let row = multi.values;
	let id = FlowId(flow::LAYOUT.get_u64(&row, ID));
	let namespace = NamespaceId(flow::LAYOUT.get_u64(&row, NAMESPACE));
	let name = flow::LAYOUT.get_utf8(&row, NAME).to_string();
	let status = FlowStatus::from_u8(flow::LAYOUT.get_u8(&row, STATUS));

	FlowDef {
		id,
		namespace,
		name,
		status,
	}
}
