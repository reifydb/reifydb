// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Re-export decode helpers from catalog.

pub(crate) use reifydb_catalog::shape::decode::{
	build_insert_diff_into_with_pool, build_remove_diff_into_with_pool, build_update_diff_into_with_pool,
};
