// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

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
	let row = multi.row;
	let id = authentication::SHAPE.get_u64(&row, authentication::ID);
	let identity = authentication::SHAPE.get_identity_id(&row, authentication::IDENTITY);
	let method = authentication::SHAPE.get_utf8(&row, authentication::METHOD).to_string();
	let properties_json = authentication::SHAPE.get_utf8(&row, authentication::PROPERTIES).to_string();

	let properties: HashMap<String, String> = from_str(&properties_json).unwrap_or_default();

	Authentication {
		id,
		identity,
		method,
		properties,
	}
}
