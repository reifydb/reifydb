// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::profile::ProfileCategoryId;
use serde::{Deserialize, Serialize};
use tracing::{Level, level_filters::LevelFilter};

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ProfileCategory {
	Query = 0,
	Txn = 1,
	Storage = 2,
	Plan = 3,
	Cdc = 4,
	Flow = 5,
}

pub const ALL_CATEGORIES: [ProfileCategory; 6] = [
	ProfileCategory::Query,
	ProfileCategory::Txn,
	ProfileCategory::Storage,
	ProfileCategory::Plan,
	ProfileCategory::Cdc,
	ProfileCategory::Flow,
];

impl ProfileCategory {
	pub const fn as_id(self) -> ProfileCategoryId {
		ProfileCategoryId(self as u8)
	}

	pub const fn from_id(id: ProfileCategoryId) -> Option<Self> {
		match id.0 {
			0 => Some(ProfileCategory::Query),
			1 => Some(ProfileCategory::Txn),
			2 => Some(ProfileCategory::Storage),
			3 => Some(ProfileCategory::Plan),
			4 => Some(ProfileCategory::Cdc),
			5 => Some(ProfileCategory::Flow),
			_ => None,
		}
	}

	pub fn from_span_name(name: &str) -> Option<Self> {
		if name.starts_with("flow::engine::") {
			Some(ProfileCategory::Flow)
		} else if name.starts_with("transaction::") {
			Some(ProfileCategory::Txn)
		} else if name.starts_with("store::single::")
			|| name.starts_with("store::multi::")
			|| name.starts_with("drop::")
		{
			Some(ProfileCategory::Storage)
		} else if name.starts_with("volcano::") || name.starts_with("vm::") {
			Some(ProfileCategory::Query)
		} else if name.starts_with("rql::") || name.starts_with("catalog::") {
			Some(ProfileCategory::Plan)
		} else if name.starts_with("cdc::") {
			Some(ProfileCategory::Cdc)
		} else {
			None
		}
	}

	pub const fn name(self) -> &'static str {
		match self {
			ProfileCategory::Query => "query",
			ProfileCategory::Txn => "txn",
			ProfileCategory::Storage => "storage",
			ProfileCategory::Plan => "plan",
			ProfileCategory::Cdc => "cdc",
			ProfileCategory::Flow => "flow",
		}
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ProfileLevel {
	Trace,
	Debug,
	Info,
	Warn,
	Error,
}

impl ProfileLevel {
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
	levels: [Option<ProfileLevel>; 6],
}

impl CategorySet {
	pub const fn empty() -> Self {
		Self {
			levels: [None; 6],
		}
	}

	pub const fn all() -> Self {
		Self::all_at(ProfileLevel::Trace)
	}

	pub const fn all_at(level: ProfileLevel) -> Self {
		Self {
			levels: [Some(level); 6],
		}
	}

	pub fn contains(&self, c: ProfileCategory) -> bool {
		self.levels[c as usize].is_some()
	}

	pub fn level_for(&self, c: ProfileCategory) -> Option<ProfileLevel> {
		self.levels[c as usize]
	}

	pub fn insert(&mut self, c: ProfileCategory) {
		self.insert_at(c, ProfileLevel::Trace);
	}

	pub fn insert_at(&mut self, c: ProfileCategory, level: ProfileLevel) {
		self.levels[c as usize] = Some(level);
	}

	pub fn remove(&mut self, c: ProfileCategory) {
		self.levels[c as usize] = None;
	}

	pub fn with(mut self, c: ProfileCategory) -> Self {
		self.insert(c);
		self
	}

	pub fn with_level(mut self, c: ProfileCategory, level: ProfileLevel) -> Self {
		self.insert_at(c, level);
		self
	}

	pub fn without(mut self, c: ProfileCategory) -> Self {
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
		assert_eq!(ProfileCategory::from_span_name("flow::engine::apply"), Some(ProfileCategory::Flow));
		assert_eq!(ProfileCategory::from_span_name("flow::engine::process_batch"), Some(ProfileCategory::Flow));
		assert_eq!(ProfileCategory::from_span_name("transaction::commit"), Some(ProfileCategory::Txn));
		assert_eq!(ProfileCategory::from_span_name("store::multi::write"), Some(ProfileCategory::Storage));
		assert_eq!(ProfileCategory::from_span_name("store::single::scan"), Some(ProfileCategory::Storage));
		assert_eq!(ProfileCategory::from_span_name("drop::range"), Some(ProfileCategory::Storage));
		assert_eq!(ProfileCategory::from_span_name("volcano::project"), Some(ProfileCategory::Query));
		assert_eq!(ProfileCategory::from_span_name("vm::executor"), Some(ProfileCategory::Query));
		assert_eq!(ProfileCategory::from_span_name("rql::parse"), Some(ProfileCategory::Plan));
		assert_eq!(ProfileCategory::from_span_name("catalog::lookup"), Some(ProfileCategory::Plan));
		assert_eq!(ProfileCategory::from_span_name("cdc::write"), Some(ProfileCategory::Cdc));
	}

	#[test]
	fn from_span_name_unknown_returns_none() {
		assert_eq!(ProfileCategory::from_span_name("random::other::name"), None);
		assert_eq!(ProfileCategory::from_span_name(""), None);
		assert_eq!(ProfileCategory::from_span_name("tracing::subsystem::start"), None);
	}

	#[test]
	fn id_roundtrip() {
		for c in ALL_CATEGORIES {
			assert_eq!(ProfileCategory::from_id(c.as_id()), Some(c));
		}
		assert_eq!(ProfileCategory::from_id(ProfileCategoryId(6)), None);
		assert_eq!(ProfileCategory::from_id(ProfileCategoryId(255)), None);
	}

	#[test]
	fn category_set_membership() {
		let mut s = CategorySet::empty();
		assert!(s.is_empty());
		s.insert(ProfileCategory::Flow);
		assert!(s.contains(ProfileCategory::Flow));
		assert!(!s.contains(ProfileCategory::Query));
		s.insert(ProfileCategory::Query);
		assert!(s.contains(ProfileCategory::Query));
		s.remove(ProfileCategory::Flow);
		assert!(!s.contains(ProfileCategory::Flow));

		let all = CategorySet::all();
		for c in ALL_CATEGORIES {
			assert!(all.contains(c));
		}
	}

	#[test]
	fn category_set_default_insert_is_trace() {
		let mut s = CategorySet::empty();
		s.insert(ProfileCategory::Flow);
		assert_eq!(s.level_for(ProfileCategory::Flow), Some(ProfileLevel::Trace));
	}

	#[test]
	fn category_set_per_category_level() {
		let mut s = CategorySet::empty();
		s.insert_at(ProfileCategory::Flow, ProfileLevel::Trace);
		s.insert_at(ProfileCategory::Query, ProfileLevel::Debug);
		s.insert_at(ProfileCategory::Storage, ProfileLevel::Info);

		assert_eq!(s.level_for(ProfileCategory::Flow), Some(ProfileLevel::Trace));
		assert_eq!(s.level_for(ProfileCategory::Query), Some(ProfileLevel::Debug));
		assert_eq!(s.level_for(ProfileCategory::Storage), Some(ProfileLevel::Info));
		assert_eq!(s.level_for(ProfileCategory::Plan), None);

		assert!(s.contains(ProfileCategory::Flow));
		assert!(s.contains(ProfileCategory::Query));
		assert!(s.contains(ProfileCategory::Storage));
		assert!(!s.contains(ProfileCategory::Plan));
	}

	#[test]
	fn category_set_with_level_round_trips() {
		let s = CategorySet::empty()
			.with_level(ProfileCategory::Flow, ProfileLevel::Trace)
			.with_level(ProfileCategory::Plan, ProfileLevel::Debug);
		assert_eq!(s.level_for(ProfileCategory::Flow), Some(ProfileLevel::Trace));
		assert_eq!(s.level_for(ProfileCategory::Plan), Some(ProfileLevel::Debug));
		assert_eq!(s.level_for(ProfileCategory::Cdc), None);
	}

	#[test]
	fn category_set_all_at_sets_uniform_level() {
		let s = CategorySet::all_at(ProfileLevel::Debug);
		for c in ALL_CATEGORIES {
			assert_eq!(s.level_for(c), Some(ProfileLevel::Debug));
		}
	}

	#[test]
	fn profile_level_admits_at_or_less_verbose() {
		assert!(ProfileLevel::Debug.admits(&Level::DEBUG));
		assert!(ProfileLevel::Debug.admits(&Level::INFO));
		assert!(ProfileLevel::Debug.admits(&Level::WARN));
		assert!(ProfileLevel::Debug.admits(&Level::ERROR));
		assert!(!ProfileLevel::Debug.admits(&Level::TRACE));

		assert!(ProfileLevel::Trace.admits(&Level::TRACE));
		assert!(!ProfileLevel::Error.admits(&Level::WARN));
	}
}
