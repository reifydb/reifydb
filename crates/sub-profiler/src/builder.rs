// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! `ProfilerConfigurator` mirrors the shape of `TracingConfigurator`: a builder fluent API the subsystem factory
//! invokes to assemble the live `ProfilerSubsystem`. Only configuration knobs live here; the actual lifecycle and
//! wiring (event bus listeners, actor spawn, layer construction) sit in `factory.rs`.

use reifydb_profiler::category::{CategorySet, ProfileCategory, ProfileLevel};

pub struct ProfilerConfigurator {
	pub enabled: bool,
	pub categories: CategorySet,
	pub accumulator_capacity: usize,
	pub min_calls_for_retention: u64,
	pub scope_local_capacity: usize,
}

impl Default for ProfilerConfigurator {
	fn default() -> Self {
		Self::new()
	}
}

impl ProfilerConfigurator {
	pub fn new() -> Self {
		Self {
			enabled: true,
			categories: CategorySet::all(),
			accumulator_capacity: 4096,
			min_calls_for_retention: 0,
			scope_local_capacity: 256,
		}
	}

	pub fn disabled(mut self) -> Self {
		self.enabled = false;
		self
	}

	pub fn with_category(mut self, c: ProfileCategory) -> Self {
		self.categories.insert(c);
		self
	}

	pub fn with_category_level(mut self, c: ProfileCategory, level: ProfileLevel) -> Self {
		self.categories.insert_at(c, level);
		self
	}

	pub fn without_category(mut self, c: ProfileCategory) -> Self {
		self.categories.remove(c);
		self
	}

	pub fn with_categories(mut self, cs: impl IntoIterator<Item = ProfileCategory>) -> Self {
		let mut set = CategorySet::empty();
		for c in cs {
			set.insert(c);
		}
		self.categories = set;
		self
	}

	pub fn with_categories_levels(mut self, cs: impl IntoIterator<Item = (ProfileCategory, ProfileLevel)>) -> Self {
		let mut set = CategorySet::empty();
		for (c, level) in cs {
			set.insert_at(c, level);
		}
		self.categories = set;
		self
	}

	pub fn with_accumulator_capacity(mut self, cap: usize) -> Self {
		self.accumulator_capacity = cap;
		self
	}

	pub fn with_min_calls_for_retention(mut self, n: u64) -> Self {
		self.min_calls_for_retention = n;
		self
	}

	pub fn with_scope_local_capacity(mut self, cap: usize) -> Self {
		self.scope_local_capacity = cap;
		self
	}
}

#[cfg(test)]
mod tests {
	use reifydb_profiler::category::ALL_CATEGORIES;

	use super::*;

	#[test]
	fn default_enables_all_categories_at_trace() {
		let cfg = ProfilerConfigurator::default();
		assert!(cfg.enabled);
		for c in ALL_CATEGORIES {
			assert!(cfg.categories.contains(c));
			assert_eq!(cfg.categories.level_for(c), Some(ProfileLevel::Trace));
		}
	}

	#[test]
	fn with_categories_replaces_set_at_trace() {
		let cfg =
			ProfilerConfigurator::new().with_categories([ProfileCategory::Query, ProfileCategory::Storage]);
		assert!(cfg.categories.contains(ProfileCategory::Query));
		assert!(cfg.categories.contains(ProfileCategory::Storage));
		assert!(!cfg.categories.contains(ProfileCategory::Flow));
		assert_eq!(cfg.categories.level_for(ProfileCategory::Query), Some(ProfileLevel::Trace));
		assert_eq!(cfg.categories.level_for(ProfileCategory::Storage), Some(ProfileLevel::Trace));
	}

	#[test]
	fn with_category_level_round_trips() {
		let cfg = ProfilerConfigurator::new()
			.with_category_level(ProfileCategory::Query, ProfileLevel::Debug)
			.with_category_level(ProfileCategory::Plan, ProfileLevel::Info);
		assert_eq!(cfg.categories.level_for(ProfileCategory::Query), Some(ProfileLevel::Debug));
		assert_eq!(cfg.categories.level_for(ProfileCategory::Plan), Some(ProfileLevel::Info));
		assert_eq!(cfg.categories.level_for(ProfileCategory::Flow), Some(ProfileLevel::Trace));
	}

	#[test]
	fn with_categories_levels_bulk_set() {
		let cfg = ProfilerConfigurator::new().with_categories_levels([
			(ProfileCategory::Flow, ProfileLevel::Trace),
			(ProfileCategory::Storage, ProfileLevel::Debug),
		]);
		assert_eq!(cfg.categories.level_for(ProfileCategory::Flow), Some(ProfileLevel::Trace));
		assert_eq!(cfg.categories.level_for(ProfileCategory::Storage), Some(ProfileLevel::Debug));
		assert_eq!(cfg.categories.level_for(ProfileCategory::Query), None);
	}
}
