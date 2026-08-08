// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashMap;

use reifydb_core::interface::{catalog::authentication::Authentication, store::MultiVersionRow};
use serde_json::from_str;

use crate::store::authentication::shape::authentication;

pub mod create;
pub mod drop;
pub mod find;
pub mod list;
pub mod shape;

pub(crate) fn convert_authentication(multi: MultiVersionRow) -> Authentication {
	let bytes = multi.bytes;
	let id = authentication::get_id(&bytes);
	let identity = authentication::get_identity(&bytes);
	let method = authentication::get_method(&bytes).to_string();
	let properties_json = authentication::get_properties(&bytes).to_string();

	let properties: HashMap<String, String> = from_str(&properties_json).unwrap_or_default();

	Authentication {
		id,
		identity,
		method,
		properties,
	}
}
