// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::catalog::EncodedCatalogRow;
use reifydb_core::{
	interface::{catalog::config::ConfigKey, store::MultiVersionRow},
	key::any::AnyKey,
};
use reifydb_value::value::Value;
use tracing::warn;

use crate::store::config::shape::config;

pub mod set;
pub mod shape;

pub(crate) fn convert_config(multi: MultiVersionRow<AnyKey>) -> Option<(ConfigKey, Value)> {
	let AnyKey::ConfigStorage(stored) = &multi.key else {
		warn!("skipping unknown persisted config key");
		return None;
	};
	let config_key = stored.key;

	let value = match config::get_value(EncodedCatalogRow::view(&multi.bytes)) {
		Value::Any(inner) => *inner,
		other => other,
	};

	Some((config_key, value))
}
