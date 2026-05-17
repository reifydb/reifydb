// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::profiler::ProfilerCategoryId;
use serde::{Deserialize, Serialize};
use tracing::{Level, level_filters::LevelFilter};

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ProfilerCategory {
	Query = 0,
	Txn = 1,
	Storage = 2,
	Plan = 3,
	Cdc = 4,
	Flow = 5,
}

pub const ALL_CATEGORIES: [ProfilerCategory; 6] = [
	ProfilerCategory::Query,
	ProfilerCategory::Txn,
	ProfilerCategory::Storage,
	ProfilerCategory::Plan,
	ProfilerCategory::Cdc,
	ProfilerCategory::Flow,
];

impl ProfilerCategory {
	pub const fn as_id(self) -> ProfilerCategoryId {
		ProfilerCategoryId(self as u8)
	}

	pub const fn from_id(id: ProfilerCategoryId) -> Option<Self> {
		match id.0 {
			0 => Some(ProfilerCategory::Query),
			1 => Some(ProfilerCategory::Txn),
			2 => Some(ProfilerCategory::Storage),
			3 => Some(ProfilerCategory::Plan),
			4 => Some(ProfilerCategory::Cdc),
			5 => Some(ProfilerCategory::Flow),
			_ => None,
		}
	}

	pub fn from_span_name(name: &str) -> Option<Self> {
		if name.starts_with("flow::engine::") {
			Some(ProfilerCategory::Flow)
		} else if name.starts_with("transaction::") {
			Some(ProfilerCategory::Txn)
		} else if name.starts_with("store::single::")
			|| name.starts_with("store::multi::")
			|| name.starts_with("drop::")
		{
			Some(ProfilerCategory::Storage)
		} else if name.starts_with("volcano::") || name.starts_with("vm::") {
			Some(ProfilerCategory::Query)
		} else if name.starts_with("rql::") || name.starts_with("catalog::") {
			Some(ProfilerCategory::Plan)
		} else if name.starts_with("cdc::") {
			Some(ProfilerCategory::Cdc)
		} else {
			None
		}
	}

	pub const fn name(self) -> &'static str {
		match self {
			ProfilerCategory::Query => "query",
			ProfilerCategory::Txn => "txn",
			ProfilerCategory::Storage => "storage",
			ProfilerCategory::Plan => "plan",
			ProfilerCategory::Cdc => "cdc",
			ProfilerCategory::Flow => "flow",
		}
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ProfilerLevel {
	Trace,
	Debug,
	Info,
	Warn,
	Error,
}

impl ProfilerLevel {
	pub fn as_level_filter(self) -> LevelFilter {
		match self {
			Self::Trace => LevelFilter::TRACE,
			Self::Debug => LevelFilter::DEBUG,
			Self::Info => LevelFilter::INFO,
			Self::Warn => LevelFilter::WARN,
			Self::Error => LevelFilter::ERROR,
		}
	}

	pub fn admits(self, level: &Level) -> bool {
		*level <= self.as_level_filter()
	}
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CategorySet {
	levels: [Option<ProfilerLevel>; 6],
}

impl CategorySet {
	pub const fn empty() -> Self {
		Self {
			levels: [None; 6],
		}
	}

	pub const fn all() -> Self {
		Self::all_at(ProfilerLevel::Trace)
	}

	pub const fn all_at(level: ProfilerLevel) -> Self {
		Self {
			levels: [Some(level); 6],
		}
	}

	pub fn contains(&self, c: ProfilerCategory) -> bool {
		self.levels[c as usize].is_some()
	}

	pub fn level_for(&self, c: ProfilerCategory) -> Option<ProfilerLevel> {
		self.levels[c as usize]
	}

	pub fn insert(&mut self, c: ProfilerCategory) {
		self.insert_at(c, ProfilerLevel::Trace);
	}

	pub fn insert_at(&mut self, c: ProfilerCategory, level: ProfilerLevel) {
		self.levels[c as usize] = Some(level);
	}

	pub fn remove(&mut self, c: ProfilerCategory) {
		self.levels[c as usize] = None;
	}

	pub fn with(mut self, c: ProfilerCategory) -> Self {
		self.insert(c);
		self
	}

	pub fn with_level(mut self, c: ProfilerCategory, level: ProfilerLevel) -> Self {
		self.insert_at(c, level);
		self
	}

	pub fn without(mut self, c: ProfilerCategory) -> Self {
		self.remove(c);
		self
	}

	pub fn is_empty(&self) -> bool {
		self.levels.iter().all(|l| l.is_none())
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn from_span_name_known_prefixes() {
		assert_eq!(ProfilerCategory::from_span_name("flow::engine::apply"), Some(ProfilerCategory::Flow));
		assert_eq!(
			ProfilerCategory::from_span_name("flow::engine::process_batch"),
			Some(ProfilerCategory::Flow)
		);
		assert_eq!(ProfilerCategory::from_span_name("transaction::commit"), Some(ProfilerCategory::Txn));
		assert_eq!(ProfilerCategory::from_span_name("store::multi::write"), Some(ProfilerCategory::Storage));
		assert_eq!(ProfilerCategory::from_span_name("store::single::scan"), Some(ProfilerCategory::Storage));
		assert_eq!(ProfilerCategory::from_span_name("drop::range"), Some(ProfilerCategory::Storage));
		assert_eq!(ProfilerCategory::from_span_name("volcano::project"), Some(ProfilerCategory::Query));
		assert_eq!(ProfilerCategory::from_span_name("vm::executor"), Some(ProfilerCategory::Query));
		assert_eq!(ProfilerCategory::from_span_name("rql::parse"), Some(ProfilerCategory::Plan));
		assert_eq!(ProfilerCategory::from_span_name("catalog::lookup"), Some(ProfilerCategory::Plan));
		assert_eq!(ProfilerCategory::from_span_name("cdc::write"), Some(ProfilerCategory::Cdc));
	}

	#[test]
	fn from_span_name_unknown_returns_none() {
		assert_eq!(ProfilerCategory::from_span_name("random::other::name"), None);
		assert_eq!(ProfilerCategory::from_span_name(""), None);
		assert_eq!(ProfilerCategory::from_span_name("tracing::subsystem::start"), None);
	}

	#[test]
	fn id_roundtrip() {
		for c in ALL_CATEGORIES {
			assert_eq!(ProfilerCategory::from_id(c.as_id()), Some(c));
		}
		assert_eq!(ProfilerCategory::from_id(ProfilerCategoryId(6)), None);
		assert_eq!(ProfilerCategory::from_id(ProfilerCategoryId(255)), None);
	}

	#[test]
	fn category_set_membership() {
		let mut s = CategorySet::empty();
		assert!(s.is_empty());
		s.insert(ProfilerCategory::Flow);
		assert!(s.contains(ProfilerCategory::Flow));
		assert!(!s.contains(ProfilerCategory::Query));
		s.insert(ProfilerCategory::Query);
		assert!(s.contains(ProfilerCategory::Query));
		s.remove(ProfilerCategory::Flow);
		assert!(!s.contains(ProfilerCategory::Flow));

		let all = CategorySet::all();
		for c in ALL_CATEGORIES {
			assert!(all.contains(c));
		}
	}

	#[test]
	fn category_set_default_insert_is_trace() {
		let mut s = CategorySet::empty();
		s.insert(ProfilerCategory::Flow);
		assert_eq!(s.level_for(ProfilerCategory::Flow), Some(ProfilerLevel::Trace));
	}

	#[test]
	fn category_set_per_category_level() {
		let mut s = CategorySet::empty();
		s.insert_at(ProfilerCategory::Flow, ProfilerLevel::Trace);
		s.insert_at(ProfilerCategory::Query, ProfilerLevel::Debug);
		s.insert_at(ProfilerCategory::Storage, ProfilerLevel::Info);

		assert_eq!(s.level_for(ProfilerCategory::Flow), Some(ProfilerLevel::Trace));
		assert_eq!(s.level_for(ProfilerCategory::Query), Some(ProfilerLevel::Debug));
		assert_eq!(s.level_for(ProfilerCategory::Storage), Some(ProfilerLevel::Info));
		assert_eq!(s.level_for(ProfilerCategory::Plan), None);

		assert!(s.contains(ProfilerCategory::Flow));
		assert!(s.contains(ProfilerCategory::Query));
		assert!(s.contains(ProfilerCategory::Storage));
		assert!(!s.contains(ProfilerCategory::Plan));
	}

	#[test]
	fn category_set_with_level_round_trips() {
		let s = CategorySet::empty()
			.with_level(ProfilerCategory::Flow, ProfilerLevel::Trace)
			.with_level(ProfilerCategory::Plan, ProfilerLevel::Debug);
		assert_eq!(s.level_for(ProfilerCategory::Flow), Some(ProfilerLevel::Trace));
		assert_eq!(s.level_for(ProfilerCategory::Plan), Some(ProfilerLevel::Debug));
		assert_eq!(s.level_for(ProfilerCategory::Cdc), None);
	}

	#[test]
	fn category_set_all_at_sets_uniform_level() {
		let s = CategorySet::all_at(ProfilerLevel::Debug);
		for c in ALL_CATEGORIES {
			assert_eq!(s.level_for(c), Some(ProfilerLevel::Debug));
		}
	}

	#[test]
	fn profile_level_admits_at_or_less_verbose() {
		assert!(ProfilerLevel::Debug.admits(&Level::DEBUG));
		assert!(ProfilerLevel::Debug.admits(&Level::INFO));
		assert!(ProfilerLevel::Debug.admits(&Level::WARN));
		assert!(ProfilerLevel::Debug.admits(&Level::ERROR));
		assert!(!ProfilerLevel::Debug.admits(&Level::TRACE));

		assert!(ProfilerLevel::Trace.admits(&Level::TRACE));
		assert!(!ProfilerLevel::Error.admits(&Level::WARN));
	}
}
