// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::{catalog::config::SystemConfigKey, store::MultiVersionRow},
	key::{EncodableKey, config::ConfigKey},
};
use reifydb_type::value::Value;

use crate::store::config::shape::config::{SHAPE, VALUE};

pub mod set;
pub mod shape;

pub(crate) fn convert_config(multi: MultiVersionRow) -> (SystemConfigKey, Value) {
	let config_key =
		ConfigKey::decode(&multi.key).map(|k| k.key).unwrap_or_else(|| panic!("failed to decode ConfigKey"));

	let value = match SHAPE.get_value(&multi.row, VALUE) {
		Value::Any(inner) => *inner,
		other => other,
	};

	(config_key, value)
}
