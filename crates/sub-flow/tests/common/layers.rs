// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb::testing::db::TestDb;
use reifydb_core::{common::SourceVersion, interface::catalog::object::ObjectId};
use reifydb_engine::lift::entry_changed_objects;

pub fn layer_sources(db: &TestDb, object: ObjectId) -> Vec<SourceVersion> {
	db.commit_history()
		.iter()
		.filter(|entry| entry_changed_objects(entry).contains(&object))
		.flat_map(|entry| {
			if entry.layers.is_empty() {
				vec![entry.version.source]
			} else {
				entry.layers.iter().map(|(source, _)| *source).collect()
			}
		})
		.collect()
}
