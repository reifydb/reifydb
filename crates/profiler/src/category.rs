// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

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
	Subscription = 6,
	Server = 7,
	Wire = 8,
	Auth = 9,
	Catalog = 10,
	Engine = 11,
	Mutate = 12,
	Transport = 13,
	Task = 14,
	Policy = 15,
	Ffi = 16,
}

pub const CATEGORY_COUNT: usize = 17;

pub const ALL_CATEGORIES: [ProfilerCategory; CATEGORY_COUNT] = [
	ProfilerCategory::Query,
	ProfilerCategory::Txn,
	ProfilerCategory::Storage,
	ProfilerCategory::Plan,
	ProfilerCategory::Cdc,
	ProfilerCategory::Flow,
	ProfilerCategory::Subscription,
	ProfilerCategory::Server,
	ProfilerCategory::Wire,
	ProfilerCategory::Auth,
	ProfilerCategory::Catalog,
	ProfilerCategory::Engine,
	ProfilerCategory::Mutate,
	ProfilerCategory::Transport,
	ProfilerCategory::Task,
	ProfilerCategory::Policy,
	ProfilerCategory::Ffi,
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
			6 => Some(ProfilerCategory::Subscription),
			7 => Some(ProfilerCategory::Server),
			8 => Some(ProfilerCategory::Wire),
			9 => Some(ProfilerCategory::Auth),
			10 => Some(ProfilerCategory::Catalog),
			11 => Some(ProfilerCategory::Engine),
			12 => Some(ProfilerCategory::Mutate),
			13 => Some(ProfilerCategory::Transport),
			14 => Some(ProfilerCategory::Task),
			15 => Some(ProfilerCategory::Policy),
			16 => Some(ProfilerCategory::Ffi),
			_ => None,
		}
	}

	pub fn from_span_name(name: &str) -> Option<Self> {
		if name.starts_with("flow::ffi::") {
			Some(ProfilerCategory::Ffi)
		} else if name.starts_with("flow::") {
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
		} else if name.starts_with("rql::") {
			Some(ProfilerCategory::Plan)
		} else if name.starts_with("catalog::") {
			Some(ProfilerCategory::Catalog)
		} else if name.starts_with("cdc::") {
			Some(ProfilerCategory::Cdc)
		} else if name.starts_with("subscription::") {
			Some(ProfilerCategory::Subscription)
		} else if name.starts_with("server::") {
			Some(ProfilerCategory::Server)
		} else if name.starts_with("wire::") {
			Some(ProfilerCategory::Wire)
		} else if name.starts_with("auth::") {
			Some(ProfilerCategory::Auth)
		} else if name.starts_with("engine::")
			|| name.starts_with("executor::")
			|| name.starts_with("session::")
		{
			Some(ProfilerCategory::Engine)
		} else if name.starts_with("mutate::") {
			Some(ProfilerCategory::Mutate)
		} else if name.starts_with("http::") || name.starts_with("dispatch::") {
			Some(ProfilerCategory::Transport)
		} else if name.starts_with("task::") {
			Some(ProfilerCategory::Task)
		} else if name.starts_with("policy::") {
			Some(ProfilerCategory::Policy)
		} else if name.starts_with("ffi::")
			|| name.starts_with("procedure::")
			|| name.starts_with("transform::")
		{
			Some(ProfilerCategory::Ffi)
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
			ProfilerCategory::Subscription => "subscription",
			ProfilerCategory::Server => "server",
			ProfilerCategory::Wire => "wire",
			ProfilerCategory::Auth => "auth",
			ProfilerCategory::Catalog => "catalog",
			ProfilerCategory::Engine => "engine",
			ProfilerCategory::Mutate => "mutate",
			ProfilerCategory::Transport => "transport",
			ProfilerCategory::Task => "task",
			ProfilerCategory::Policy => "policy",
			ProfilerCategory::Ffi => "ffi",
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
	levels: [Option<ProfilerLevel>; CATEGORY_COUNT],
}

impl CategorySet {
	pub const fn empty() -> Self {
		Self {
			levels: [None; CATEGORY_COUNT],
		}
	}

	pub const fn all() -> Self {
		Self::all_at(ProfilerLevel::Trace)
	}

	pub const fn all_at(level: ProfilerLevel) -> Self {
		Self {
			levels: [Some(level); CATEGORY_COUNT],
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
		assert_eq!(ProfilerCategory::from_span_name("cdc::write"), Some(ProfilerCategory::Cdc));
		assert_eq!(
			ProfilerCategory::from_span_name("subscription::consume"),
			Some(ProfilerCategory::Subscription)
		);
		assert_eq!(ProfilerCategory::from_span_name("server::deliver"), Some(ProfilerCategory::Server));
		assert_eq!(ProfilerCategory::from_span_name("wire::encode_frames"), Some(ProfilerCategory::Wire));
		assert_eq!(ProfilerCategory::from_span_name("auth::authenticate"), Some(ProfilerCategory::Auth));
		assert_eq!(ProfilerCategory::from_span_name("engine::query_as"), Some(ProfilerCategory::Engine));
		assert_eq!(ProfilerCategory::from_span_name("executor::compile"), Some(ProfilerCategory::Engine));
		assert_eq!(ProfilerCategory::from_span_name("session::query"), Some(ProfilerCategory::Engine));
		assert_eq!(ProfilerCategory::from_span_name("mutate::table::insert"), Some(ProfilerCategory::Mutate));
		assert_eq!(ProfilerCategory::from_span_name("http::query"), Some(ProfilerCategory::Transport));
		assert_eq!(
			ProfilerCategory::from_span_name("dispatch::send_server_message"),
			Some(ProfilerCategory::Transport)
		);
		assert_eq!(ProfilerCategory::from_span_name("task::spawn"), Some(ProfilerCategory::Task));
		assert_eq!(ProfilerCategory::from_span_name("policy::enforce"), Some(ProfilerCategory::Policy));
		assert_eq!(ProfilerCategory::from_span_name("ffi::callback"), Some(ProfilerCategory::Ffi));
		assert_eq!(ProfilerCategory::from_span_name("procedure::ffi::execute"), Some(ProfilerCategory::Ffi));
		assert_eq!(ProfilerCategory::from_span_name("transform::ffi::apply"), Some(ProfilerCategory::Ffi));
		// flow::ffi:: must beat the broader flow:: -> Flow rule so FFI boundary cost is attributed to Ffi.
		assert_eq!(ProfilerCategory::from_span_name("flow::ffi::vtable_call"), Some(ProfilerCategory::Ffi));
		assert_eq!(ProfilerCategory::from_span_name("flow::engine::apply"), Some(ProfilerCategory::Flow));
	}

	#[test]
	fn catalog_split_out_of_plan() {
		// catalog:: was moved out of Plan into its own Catalog category so Plan reflects
		// real query planning (rql::) rather than being dominated by metadata lookups.
		assert_eq!(
			ProfilerCategory::from_span_name("catalog::column::find_by_name"),
			Some(ProfilerCategory::Catalog)
		);
		assert_eq!(ProfilerCategory::from_span_name("rql::plan"), Some(ProfilerCategory::Plan));
	}

	#[test]
	fn from_span_name_flow_covers_non_engine_prefixes() {
		// Flow was widened from `flow::engine::` to `flow::` so the already-instrumented
		// coordinator/pool/worker spans are captured, not just the engine internals.
		assert_eq!(
			ProfilerCategory::from_span_name("flow::coordinator::consume"),
			Some(ProfilerCategory::Flow)
		);
		assert_eq!(ProfilerCategory::from_span_name("flow::pool::submit"), Some(ProfilerCategory::Flow));
		assert_eq!(ProfilerCategory::from_span_name("flow::actor::tick"), Some(ProfilerCategory::Flow));
		assert_eq!(ProfilerCategory::from_span_name("flow::engine::apply"), Some(ProfilerCategory::Flow));
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
		assert_eq!(ProfilerCategory::from_id(ProfilerCategoryId(CATEGORY_COUNT as u8)), None);
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
