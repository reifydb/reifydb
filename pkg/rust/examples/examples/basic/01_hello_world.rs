// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb::{IdentityId, Params, embedded};
use reifydb_examples::log_query;
use tracing::info;

fn main() {
	// Step 1: an in-memory database - nothing is persisted, all state lives in the process.
	let db = embedded::memory().build().unwrap();

	// Step 2: a COMMAND may modify state. MAP builds a result set out of computed values.
	log_query("MAP { answer: 42 }");
	let frames = db.command_as_root("MAP { answer: 42 }", Params::None).unwrap();
	for frame in frames {
		info!("{}", frame);
	}

	// Step 3: a QUERY is read-only - it computes and returns without touching state.
	log_query("Map { another_answer: 40 + 2 }");
	let frames = db.query_as_root("Map { another_answer: 40 + 2 }", Params::None).unwrap();
	for frame in frames {
		info!("{}", frame);
	}

	// Step 4: a SESSION gives an isolated execution context with its own identity and
	// permissions, and can carry state across several operations.
	info!("Creating a session for isolated operations");
	let session = db.session(IdentityId::root());

	log_query("map { yet_another_answer: 20 * 2 + 2 }");
	let r = session.query("map { yet_another_answer: 20 * 2 + 2 }", Params::None);
	if let Some(e) = r.error {
		panic!("query failed: {e:?}");
	}
	let frames = r.frames;
	for frame in frames {
		info!("{}", frame);
	}

	// Dropping the database closes it and releases its resources.
	info!("Shutting down database...");
	drop(db);
}
