// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb::{ConfigKey, Value, embedded, value::duration::Duration};

#[test]
fn test_with_config_applied_at_bootstrap() {
	let one_hour = Duration::from_hours(1).unwrap();
	let db = embedded::memory()
		.with_config(ConfigKey::CdcTtlDuration, Value::Duration(one_hour.clone()))
		.build()
		.unwrap();

	let value = db.engine().catalog().materialized.get_config(ConfigKey::CdcTtlDuration);
	assert!(matches!(value, Value::Duration(d) if d == one_hour));
}

#[test]
fn test_with_configs_applies_multiple() {
	let db = embedded::memory()
		.with_configs([
			(ConfigKey::OracleWindowSize, Value::Uint8(1000)),
			(ConfigKey::CdcTtlDuration, Value::Duration(Duration::from_hours(2).unwrap())),
		])
		.build()
		.unwrap();

	let catalog = db.engine().catalog();
	assert!(matches!(catalog.materialized.get_config(ConfigKey::OracleWindowSize), Value::Uint8(1000)));
	assert!(matches!(
		catalog.materialized.get_config(ConfigKey::CdcTtlDuration),
		Value::Duration(d) if d == Duration::from_hours(2).unwrap()
	));
}

#[test]
fn test_invalid_value_fails_at_build() {
	let zero = Duration::from_seconds(0).unwrap();
	let result = embedded::memory().with_config(ConfigKey::CdcTtlDuration, Value::Duration(zero)).build();

	assert!(result.is_err(), "expected zero CDC TTL to be rejected at bootstrap");
}

#[test]
fn test_defaults_preserved_when_no_override() {
	let db = embedded::memory().build().unwrap();
	let value = db.engine().catalog().materialized.get_config(ConfigKey::CdcTtlDuration);
	assert!(matches!(value, Value::None { .. }));
}
