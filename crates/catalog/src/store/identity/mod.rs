// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::interface::{catalog::identity::Identity, store::MultiVersionRow};

use crate::store::identity::shape::identity;

pub mod create;
pub mod drop;
pub mod find;
pub mod list;
pub mod shape;

pub(crate) fn convert_identity(multi: MultiVersionRow) -> Identity {
	let bytes = multi.bytes;
	let id = identity::get_identity(&bytes);
	let name = identity::get_name(&bytes).to_string();
	let enabled = identity::get_enabled(&bytes);

	Identity {
		id,
		name,
		enabled,
	}
}
