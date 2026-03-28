// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! In-memory registry for runtime database configuration.
//!
//! `ConfigRegistry` holds all registered configuration entries with their
//! current values. Subsystems register their configs at startup, and the
//! bootstrap process applies any persisted overrides from storage.

use std::{
	fmt,
	fmt::{Debug, Formatter},
	sync::Arc,
};

use crossbeam_skiplist::SkipMap;
use reifydb_type::value::Value;

use crate::{common::CommitVersion, interface::catalog::config::Config, util::multi::MultiVersionContainer};

/// A single configuration entry in the registry.
pub struct ConfigEntry {
	/// Compile-time default value
	pub default_value: Value,
	/// Human-readable description
	pub description: &'static str,
	/// Whether a restart is required to apply this setting
	pub requires_restart: bool,
	/// Multi-version history of values for MVCC snapshot isolation.
	pub versions: MultiVersionContainer<Value>,
}

fn is_valid_config_key(key: &str) -> bool {
	!key.is_empty()
		&& key.bytes().all(|b| b.is_ascii_uppercase() || b == b'_' || b.is_ascii_digit())
		&& key.as_bytes()[0].is_ascii_uppercase()
}

struct SystemConfigInner {
	entries: SkipMap<String, ConfigEntry>,
}

/// Registry of all runtime-tunable configuration entries.
///
/// Subsystems call `register()` at startup to declare their tunable parameters.
/// The bootstrap process calls `apply_persisted()` to override defaults with
/// values loaded from storage.
///
/// `SystemConfig` is cheaply cloneable — cloning increments an internal `Arc`
/// reference count without copying any data.
#[derive(Clone)]
pub struct SystemConfig(Arc<SystemConfigInner>);

impl Debug for SystemConfig {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		f.debug_struct("SystemConfig").finish()
	}
}

impl SystemConfig {
	pub fn new() -> Self {
		Self(Arc::new(SystemConfigInner {
			entries: SkipMap::new(),
		}))
	}

	/// Register a configuration key with its default value and metadata.
	///
	/// The default value is inserted at CommitVersion(0) so it is visible
	/// to all transactions. If the key is already registered, this is a no-op.
	pub fn register(&self, key: &str, default: Value, description: &'static str, requires_restart: bool) {
		debug_assert!(is_valid_config_key(key), "config key must be SCREAMING_SNAKE_CASE, got: {key:?}");
		if self.0.entries.contains_key(key) {
			return;
		}

		let versions = MultiVersionContainer::new();
		versions.insert(CommitVersion(0), default.clone());
		self.0.entries.insert(
			key.to_string(),
			ConfigEntry {
				default_value: default,
				description,
				requires_restart,
				versions,
			},
		);
	}

	/// Apply a persisted value loaded from storage during bootstrap.
	///
	/// If the key is not registered, the value is silently ignored
	/// (it may be from a removed config that no longer exists).
	pub fn apply_persisted(&self, key: &str, version: CommitVersion, value: Value) {
		if let Some(entry) = self.0.entries.get(key) {
			entry.value().versions.insert(version, value);
		}
	}

	/// Update a config value at the given commit version (called from post-commit interceptor).
	///
	/// Panics if the key is not registered — callers must verify via `get()`
	/// before calling this.
	pub fn update(&self, key: &str, version: CommitVersion, value: Value) {
		match self.0.entries.get(key) {
			Some(entry) => {
				entry.value().versions.insert(version, value);
			}
			None => panic!("SystemConfig::update called with unregistered key: {key}"),
		}
	}

	/// Get the latest value for a config key.
	pub fn get(&self, key: &str) -> Option<Value> {
		self.0.entries.get(key).and_then(|entry| entry.value().versions.get_latest())
	}

	/// Get the registered default value for a config key.
	pub fn get_default(&self, key: &str) -> Option<Value> {
		self.0.entries.get(key).map(|entry| entry.value().default_value.clone())
	}

	/// Get the value for a config key as of a specific snapshot version.
	pub fn get_at(&self, key: &str, version: CommitVersion) -> Option<Value> {
		self.0.entries.get(key).and_then(|entry| entry.value().versions.get(version))
	}

	/// Get the current value as a u64, or None if the key is not registered.
	/// Panics if the key is registered but the value is not Value::Uint8.
	pub fn get_uint8(&self, key: &str) -> Option<u64> {
		self.0.entries.get(key).map(|entry| match entry.value().versions.get_latest() {
			Some(Value::Uint8(v)) => v,
			Some(v) => panic!("config key '{key}' expected Uint8, got {v:?}"),
			None => panic!("config key '{key}' has no value"),
		})
	}

	/// Get the current value as a u64, panicking if the key is not registered
	/// or the value is not Value::Uint8.
	pub fn require_uint8(&self, key: &str) -> u64 {
		self.get_uint8(key).unwrap_or_else(|| panic!("config key '{key}' is not registered"))
	}

	/// List all registered configuration entries with their latest values.
	pub fn list_all(&self) -> Vec<Config> {
		self.0.entries
			.iter()
			.filter_map(|entry| {
				entry.value().versions.get_latest().map(|current| Config {
					key: entry.key().clone(),
					value: current,
					default_value: entry.value().default_value.clone(),
					description: entry.value().description,
					requires_restart: entry.value().requires_restart,
				})
			})
			.collect()
	}

	/// List all registered configuration entries with values as of a specific snapshot version.
	pub fn list_all_at(&self, version: CommitVersion) -> Vec<Config> {
		self.0.entries
			.iter()
			.filter_map(|entry| {
				entry.value().versions.get(version).map(|current| Config {
					key: entry.key().clone(),
					value: current,
					default_value: entry.value().default_value.clone(),
					description: entry.value().description,
					requires_restart: entry.value().requires_restart,
				})
			})
			.collect()
	}
}

impl Default for SystemConfig {
	fn default() -> Self {
		Self::new()
	}
}
