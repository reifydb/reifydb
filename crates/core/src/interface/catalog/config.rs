// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{fmt, str::FromStr, time::Duration as StdDuration};

use reifydb_type::value::{Value, duration::Duration, r#type::Type};

use crate::common::CommitVersion;

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum SystemConfigKey {
	OracleWindowSize,
	OracleWaterMark,
	RowTtlScanBatchSize,
	RowTtlScanInterval,
}

impl SystemConfigKey {
	pub fn all() -> &'static [Self] {
		&[Self::OracleWindowSize, Self::OracleWaterMark, Self::RowTtlScanBatchSize, Self::RowTtlScanInterval]
	}

	pub fn default_value(&self) -> Value {
		match self {
			Self::OracleWindowSize => Value::Uint8(500),
			Self::OracleWaterMark => Value::Uint8(20),
			Self::RowTtlScanBatchSize => Value::Uint8(10000),
			Self::RowTtlScanInterval => Value::Duration(Duration::from_seconds(60).unwrap()),
		}
	}

	pub fn description(&self) -> &'static str {
		match self {
			Self::OracleWindowSize => "Number of transactions per conflict-detection window.",
			Self::OracleWaterMark => "Number of conflict windows retained before cleanup is triggered.",
			Self::RowTtlScanBatchSize => "Max rows to examine per batch during a row TTL scan.",
			Self::RowTtlScanInterval => "How often the row TTL actor should scan for expired rows.",
		}
	}

	pub fn requires_restart(&self) -> bool {
		match self {
			Self::OracleWindowSize => false,
			Self::OracleWaterMark => false,
			Self::RowTtlScanBatchSize => false,
			Self::RowTtlScanInterval => false,
		}
	}

	pub fn expected_types(&self) -> &'static [Type] {
		match self {
			Self::OracleWindowSize => &[Type::Uint8],
			Self::OracleWaterMark => &[Type::Uint8],
			Self::RowTtlScanBatchSize => &[Type::Uint8],
			Self::RowTtlScanInterval => &[Type::Duration],
		}
	}
}

impl fmt::Display for SystemConfigKey {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::OracleWindowSize => write!(f, "ORACLE_WINDOW_SIZE"),
			Self::OracleWaterMark => write!(f, "ORACLE_WATER_MARK"),
			Self::RowTtlScanBatchSize => write!(f, "ROW_TTL_SCAN_BATCH_SIZE"),
			Self::RowTtlScanInterval => write!(f, "ROW_TTL_SCAN_INTERVAL"),
		}
	}
}

impl FromStr for SystemConfigKey {
	type Err = String;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		match s {
			"ORACLE_WINDOW_SIZE" => Ok(Self::OracleWindowSize),
			"ORACLE_WATER_MARK" => Ok(Self::OracleWaterMark),
			"ROW_TTL_SCAN_BATCH_SIZE" => Ok(Self::RowTtlScanBatchSize),
			"ROW_TTL_SCAN_INTERVAL" => Ok(Self::RowTtlScanInterval),
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
pub struct SystemConfig {
	/// System configuration key
	pub key: SystemConfigKey,
	/// Currently active value (persisted override or default)
	pub value: Value,
	/// Compile-time default value
	pub default_value: Value,
	/// Human-readable description
	pub description: &'static str,
	/// Whether changing this setting requires a database restart
	pub requires_restart: bool,
}

/// Trait for fetching system configuration values.
pub trait GetSystemConfig: Send + Sync {
	/// Get the configuration value at the current snapshot.
	fn get_system_config(&self, key: SystemConfigKey) -> Value;
	/// Get the configuration value at a specific snapshot version.
	fn get_system_config_at(&self, key: SystemConfigKey, version: CommitVersion) -> Value;

	/// Get the current value as a u64. Panics if the value is not Value::Uint8.
	fn get_system_config_uint8(&self, key: SystemConfigKey) -> u64 {
		let val = self.get_system_config(key);
		match val {
			Value::Uint8(v) => v,
			v => panic!("config key '{}' expected Uint8, got {:?}", key, v),
		}
	}

	/// Get the current value as a std::time::Duration. Panics if the value is not Value::Duration.
	fn get_system_config_duration(&self, key: SystemConfigKey) -> StdDuration {
		let val = self.get_system_config(key);
		match val {
			Value::Duration(v) => {
				let total_nanos =
					(v.get_days() as i128 * 24 * 3600 * 1_000_000_000) + (v.get_nanos() as i128);
				StdDuration::from_nanos(total_nanos.max(0) as u64)
			}
			v => panic!("config key '{}' expected Duration, got {:?}", key, v),
		}
	}
}
