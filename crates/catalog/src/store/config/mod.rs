// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	interface::{catalog::config::ConfigKey, store::MultiVersionRow},
	key::{EncodableKey, config::ConfigStorageKey},
};
use reifydb_value::value::Value;
use tracing::warn;

use crate::store::config::shape::config;

pub mod set;
pub mod shape;

pub(crate) fn convert_config(multi: MultiVersionRow) -> Option<(ConfigKey, Value)> {
	let config_key = match ConfigStorageKey::decode(&multi.key) {
		Some(k) => k.key,
		None => {
			warn!("skipping unknown persisted config key");
			return None;
		}
	};

	let value = match config::get_value(&multi.bytes) {
		Value::Any(inner) => *inner,
		other => other,
	};

	Some((config_key, value))
}
