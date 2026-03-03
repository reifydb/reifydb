// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_type::value::Value;

/// A configuration definition for a runtime-tunable database setting.
///
/// `value` is the currently active value (either the persisted override or the default).
/// `default_value`, `description`, and `requires_restart` are compile-time constants
/// provided at registration time — they are never stored to disk.
#[derive(Debug, Clone)]
pub struct ConfigDef {
	/// SCREAMING_SNAKE_CASE key, e.g. "ORACLE_WINDOW_SIZE"
	pub key: String,
	/// Currently active value (persisted override or default)
	pub value: Value,
	/// Compile-time default value
	pub default_value: Value,
	/// Human-readable description
	pub description: &'static str,
	/// Whether changing this setting requires a database restart
	pub requires_restart: bool,
}
