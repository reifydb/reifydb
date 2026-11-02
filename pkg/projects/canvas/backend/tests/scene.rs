// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb::{Clock, Database, IdentityId, MockClock, RuntimeConfig, WithSubsystem, server, value::params::Params};
use reifydb_canvas::{migration_path, routines};

fn build() -> Database {
	let config = RuntimeConfig {
		clock: Clock::Mock(MockClock::from_millis(1_700_000_000_000)),
		..Default::default()
	};
	server::memory()
		.with_runtime_config(config)
		.with_flow(|f| f)
		.with_routines(routines)
		.with_migrations(migration_path())
		.build()
		.expect("build memory db")
}

#[test]
fn anyone_can_ask_for_the_scene_and_gets_a_deep_space_description() {
	// Agents call this before logging in, so an anonymous caller must get the brief, not a policy error.
	let db = build();
	let r = db.engine().query_as(IdentityId::anonymous(), "CALL canvas::scene()", Params::None);
	if let Some(e) = r.error {
		panic!("scene call failed: {e:?}");
	}
	let rows: Vec<_> = r.frames.first().expect("one frame").rows().collect();
	assert_eq!(rows.len(), 1, "the scene must be exactly one row");
	let scene = rows[0].get::<String>("scene").expect("scene column").expect("scene is not none");
	assert!(scene.contains("Deep space"), "scene must describe deep space, got: {scene}");
	assert!(scene.contains("nebula"), "scene must mention the nebulae, got: {scene}");
}
