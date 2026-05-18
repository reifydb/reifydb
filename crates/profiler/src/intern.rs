// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{
	fmt,
	sync::atomic::{AtomicU32, Ordering},
};

use dashmap::DashMap;
use reifydb_runtime::sync::rwlock::RwLock;

use crate::record::{DIM_UNSET, DimIdx};

pub struct DimInterner {
	forward: DashMap<String, DimIdx>,
	reverse: RwLock<Vec<String>>,
	next: AtomicU32,
}

impl fmt::Debug for DimInterner {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("DimInterner").field("len", &self.len()).finish()
	}
}

impl Default for DimInterner {
	fn default() -> Self {
		Self::new()
	}
}

impl DimInterner {
	pub fn new() -> Self {
		Self {
			forward: DashMap::new(),
			reverse: RwLock::new(vec![String::new()]),
			next: AtomicU32::new(1),
		}
	}

	pub fn get(&self, s: &str) -> Option<DimIdx> {
		self.forward.get(s).map(|r| *r.value())
	}

	pub fn intern(&self, s: &str) -> DimIdx {
		if let Some(idx) = self.get(s) {
			return idx;
		}
		let idx = self.next.fetch_add(1, Ordering::Relaxed);
		self.forward.insert(s.to_string(), idx);
		let mut rev = self.reverse.write();
		if rev.len() as u32 <= idx {
			rev.resize((idx + 1) as usize, String::new());
		}
		rev[idx as usize] = s.to_string();
		idx
	}

	pub fn resolve(&self, idx: DimIdx) -> Option<String> {
		if idx == DIM_UNSET {
			return None;
		}
		let rev = self.reverse.read();
		rev.get(idx as usize).filter(|s| !s.is_empty()).cloned()
	}

	pub fn len(&self) -> usize {
		(self.next.load(Ordering::Relaxed) as usize).saturating_sub(1)
	}

	pub fn is_empty(&self) -> bool {
		self.len() == 0
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn get_on_uninterned_returns_none_without_inserting() {
		let interner = DimInterner::new();
		assert_eq!(interner.get("never_seen"), None);
		assert!(interner.is_empty());
	}

	#[test]
	fn intern_assigns_stable_indices() {
		let interner = DimInterner::new();
		let a = interner.intern("alpha");
		let b = interner.intern("beta");
		let a2 = interner.intern("alpha");
		assert_eq!(a, a2);
		assert_ne!(a, b);
		assert_ne!(a, DIM_UNSET);
		assert_ne!(b, DIM_UNSET);
	}

	#[test]
	fn resolve_returns_original_string() {
		let interner = DimInterner::new();
		let idx = interner.intern("hello");
		assert_eq!(interner.resolve(idx).as_deref(), Some("hello"));
		assert_eq!(interner.resolve(DIM_UNSET), None);
		assert_eq!(interner.resolve(99999), None);
	}
}
