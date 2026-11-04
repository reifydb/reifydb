// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::catalog::EncodedCatalogRow;
pub mod create;
pub mod drop;
pub mod find;
pub mod get;
pub mod list;
pub mod load;
pub(crate) mod shape;
pub mod update;

use reifydb_core::{
	interface::catalog::{
		flow::{Flow, FlowId, FlowStatus},
		id::NamespaceId,
	},
	return_internal_error,
};

use crate::{Result, store::flow::shape::flow};

pub(crate) fn decode_flow(bytes: &EncodedCatalogRow) -> Result<Flow> {
	let id = FlowId(flow::get_id(bytes));
	let byte = flow::get_status(bytes);
	let reason = flow::try_get_reason(bytes);
	let Some(status) = FlowStatus::from_stored(byte, reason) else {
		return_internal_error!(
			"Flow {} carries status byte {} with reason {:?}. This indicates a corrupt flow row.",
			id.0,
			byte,
			reason
		)
	};
	Ok(Flow {
		id,
		namespace: NamespaceId(flow::get_namespace(bytes)),
		name: flow::get_name(bytes).to_string(),
		status,
	})
}
