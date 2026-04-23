// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{fmt, str::FromStr, time::Duration as StdDuration};

use reifydb_type::value::{Value, duration::Duration, r#type::Type};

use crate::common::CommitVersion;

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum ConfigKey {
	OracleWindowSize,
	OracleWaterMark,
	RowTtlScanBatchSize,
	RowTtlScanInterval,
	CdcTtlDuration,
}

impl ConfigKey {
	pub fn all() -> &'static [Self] {
		&[
			Self::OracleWindowSize,
			Self::OracleWaterMark,
			Self::RowTtlScanBatchSize,
			Self::RowTtlScanInterval,
			Self::CdcTtlDuration,
		]
	}

	pub fn default_value(&self) -> Value {
		match self {
			Self::OracleWindowSize => Value::Uint8(500),
			Self::OracleWaterMark => Value::Uint8(20),
			Self::RowTtlScanBatchSize => Value::Uint8(10000),
			Self::RowTtlScanInterval => Value::Duration(Duration::from_seconds(60).unwrap()),
			Self::CdcTtlDuration => Value::None {
				inner: Type::Duration,
			},
		}
	}

	pub fn description(&self) -> &'static str {
		match self {
			Self::OracleWindowSize => "Number of transactions per conflict-detection window.",
			Self::OracleWaterMark => "Number of conflict windows retained before cleanup is triggered.",
			Self::RowTtlScanBatchSize => "Max rows to examine per batch during a row TTL scan.",
			Self::RowTtlScanInterval => "How often the row TTL actor should scan for expired rows.",
			Self::CdcTtlDuration => {
				"Maximum age of CDC entries before eviction. When unset, CDC is retained forever; \
				 when set, must be > 0 and entries older than this duration are evicted regardless \
				 of consumer state."
			}
		}
	}

	pub fn requires_restart(&self) -> bool {
		match self {
			Self::OracleWindowSize => false,
			Self::OracleWaterMark => false,
			Self::RowTtlScanBatchSize => false,
			Self::RowTtlScanInterval => false,
			Self::CdcTtlDuration => false,
		}
	}

	pub fn expected_types(&self) -> &'static [Type] {
		match self {
			Self::OracleWindowSize => &[Type::Uint8],
			Self::OracleWaterMark => &[Type::Uint8],
			Self::RowTtlScanBatchSize => &[Type::Uint8],
			Self::RowTtlScanInterval => &[Type::Duration],
			Self::CdcTtlDuration => &[Type::Duration],
		}
	}

	/// Whether this key may be unset to a typed-null `Value::None`.
	///
	/// Optional keys treat `Value::None { inner }` as valid as long as `inner` matches
	/// `expected_types`. Non-optional keys reject any `Value::None`.
	pub fn is_optional(&self) -> bool {
		match self {
			Self::OracleWindowSize => false,
			Self::OracleWaterMark => false,
			Self::RowTtlScanBatchSize => false,
			Self::RowTtlScanInterval => false,
			Self::CdcTtlDuration => true,
		}
	}

	/// Per-key value validation beyond type checking.
	///
	/// Returns `Err(reason)` when the value is the right type but otherwise invalid for this key.
	/// The caller is expected to wrap the reason in a domain error (e.g. `CatalogError`).
	pub fn validate(&self, value: &Value) -> Result<(), String> {
		match self {
			Self::CdcTtlDuration => match value {
				Value::None {
					..
				} => Ok(()),
				Value::Duration(d) => {
					if d.is_positive() {
						Ok(())
					} else {
						Err("CDC_TTL_DURATION must be greater than zero".to_string())
					}
				}
				_ => Ok(()),
			},
			_ => Ok(()),
		}
	}
}

impl fmt::Display for ConfigKey {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::OracleWindowSize => write!(f, "ORACLE_WINDOW_SIZE"),
			Self::OracleWaterMark => write!(f, "ORACLE_WATER_MARK"),
			Self::RowTtlScanBatchSize => write!(f, "ROW_TTL_SCAN_BATCH_SIZE"),
			Self::RowTtlScanInterval => write!(f, "ROW_TTL_SCAN_INTERVAL"),
			Self::CdcTtlDuration => write!(f, "CDC_TTL_DURATION"),
		}
	}
}

impl FromStr for ConfigKey {
	type Err = String;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		match s {
			"ORACLE_WINDOW_SIZE" => Ok(Self::OracleWindowSize),
			"ORACLE_WATER_MARK" => Ok(Self::OracleWaterMark),
			"ROW_TTL_SCAN_BATCH_SIZE" => Ok(Self::RowTtlScanBatchSize),
			"ROW_TTL_SCAN_INTERVAL" => Ok(Self::RowTtlScanInterval),
			"CDC_TTL_DURATION" => Ok(Self::CdcTtlDuration),
			_ => Err(format!("Unknown system configuration key: {}", s)),
		}
	}
}

/// A configuration definition for a runtime-tunable database setting.
///
/// `value` is the currently active value (either the persisted override or the default).
/// `default_value`, `description`, and `requires_restart` are compile-time constants
/// provided at registration time — they are never stored to disk.
#[derive(Debug, Clone)]
pub struct Config {
	/// System configuration key
	pub key: ConfigKey,
	/// Currently active value (persisted override or default)
	pub value: Value,
	/// Compile-time default value
	pub default_value: Value,
	/// Human-readable description
	pub description: &'static str,
	/// Whether changing this setting requires a database restart
	pub requires_restart: bool,
}

/// Trait for fetching configuration values.
pub trait GetConfig: Send + Sync {
	/// Get the configuration value at the current snapshot.
	fn get_config(&self, key: ConfigKey) -> Value;
	/// Get the configuration value at a specific snapshot version.
	fn get_config_at(&self, key: ConfigKey, version: CommitVersion) -> Value;

	/// Get the current value as a u64. Panics if the value is not Value::Uint8.
	fn get_config_uint8(&self, key: ConfigKey) -> u64 {
		let val = self.get_config(key);
		match val {
			Value::Uint8(v) => v,
			v => panic!("config key '{}' expected Uint8, got {:?}", key, v),
		}
	}

	/// Get the current value as a std::time::Duration. Panics if the value is not Value::Duration.
	fn get_config_duration(&self, key: ConfigKey) -> StdDuration {
		let val = self.get_config(key);
		match val {
			Value::Duration(v) => {
				let total_nanos =
					(v.get_days() as i128 * 24 * 3600 * 1_000_000_000) + (v.get_nanos() as i128);
				StdDuration::from_nanos(total_nanos.max(0) as u64)
			}
			v => panic!("config key '{}' expected Duration, got {:?}", key, v),
		}
	}

	/// Get the current value as an `Option<StdDuration>` for keys that may be unset.
	/// `None` for `Value::None`, `Some(d)` for `Value::Duration(d)`. Panics on any other variant.
	fn get_config_duration_opt(&self, key: ConfigKey) -> Option<StdDuration> {
		match self.get_config(key) {
			Value::None {
				..
			} => None,
			Value::Duration(v) => {
				let total_nanos =
					(v.get_days() as i128 * 24 * 3600 * 1_000_000_000) + (v.get_nanos() as i128);
				Some(StdDuration::from_nanos(total_nanos.max(0) as u64))
			}
			v => panic!("config key '{}' expected Duration or None, got {:?}", key, v),
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_cdc_ttl_default_is_typed_null() {
		// Defaulting to Value::None means "TTL not configured" — producer skips cleanup.
		let default = ConfigKey::CdcTtlDuration.default_value();
		assert!(matches!(
			default,
			Value::None {
				inner: Type::Duration
			}
		));
	}

	#[test]
	fn test_cdc_ttl_validate_accepts_none() {
		let none = Value::None {
			inner: Type::Duration,
		};
		assert!(ConfigKey::CdcTtlDuration.validate(&none).is_ok());
	}

	#[test]
	fn test_cdc_ttl_validate_accepts_positive_duration() {
		let one_sec = Value::Duration(Duration::from_seconds(1).unwrap());
		assert!(ConfigKey::CdcTtlDuration.validate(&one_sec).is_ok());

		let one_hour = Value::Duration(Duration::from_seconds(3600).unwrap());
		assert!(ConfigKey::CdcTtlDuration.validate(&one_hour).is_ok());
	}

	#[test]
	fn test_cdc_ttl_validate_rejects_zero() {
		let zero = Value::Duration(Duration::from_seconds(0).unwrap());
		let err = ConfigKey::CdcTtlDuration.validate(&zero).unwrap_err();
		assert!(err.contains("greater than zero"), "unexpected reason: {err}");
	}

	#[test]
	fn test_cdc_ttl_validate_rejects_negative() {
		let negative = Value::Duration(Duration::from_seconds(-5).unwrap());
		assert!(ConfigKey::CdcTtlDuration.validate(&negative).is_err());
	}

	#[test]
	fn test_other_keys_validate_unconditionally_ok() {
		// Keys without bespoke validation should accept any in-type value.
		assert!(ConfigKey::OracleWindowSize.validate(&Value::Uint8(0)).is_ok());
		assert!(ConfigKey::RowTtlScanInterval
			.validate(&Value::Duration(Duration::from_seconds(0).unwrap()))
			.is_ok());
	}

	#[test]
	fn test_cdc_ttl_round_trips_through_display_and_from_str() {
		let key: ConfigKey = "CDC_TTL_DURATION".parse().unwrap();
		assert_eq!(key, ConfigKey::CdcTtlDuration);
		assert_eq!(format!("{}", ConfigKey::CdcTtlDuration), "CDC_TTL_DURATION");
	}

	#[test]
	fn test_cdc_ttl_in_all() {
		assert!(ConfigKey::all().contains(&ConfigKey::CdcTtlDuration));
	}
}
