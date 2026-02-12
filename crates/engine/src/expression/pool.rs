// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::{cell::RefCell, collections::HashMap};

use reifydb_core::value::column::data::ColumnData;
use reifydb_type::value::r#type::Type;

thread_local! {
	static POOL: RefCell<HashMap<Type, Vec<ColumnData>>> = RefCell::new(HashMap::new());
}

/// Take a cleared `ColumnData` of the given type from the pool,
/// or allocate a new one if the pool is empty for that type.
pub fn take(ty: Type, capacity: usize) -> ColumnData {
	POOL.with(|pool| {
		let mut pool = pool.borrow_mut();
		if let Some(vec) = pool.get_mut(&ty) {
			if let Some(mut data) = vec.pop() {
				data.clear();
				return data;
			}
		}
		ColumnData::with_capacity(ty, capacity)
	})
}

/// Return a `ColumnData` to the pool for later reuse.
/// Skips pooling for `Undefined` type since those are trivially cheap.
pub fn recycle(data: ColumnData) {
	let ty = data.get_type();
	if matches!(ty, Type::Undefined) {
		return;
	}
	POOL.with(|pool| {
		pool.borrow_mut().entry(ty).or_default().push(data);
	});
}
