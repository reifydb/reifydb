// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb::{Params, embedded};
use reifydb_examples::log_query;
use tracing::info;

fn main() {
	let db = embedded::memory().build().unwrap();

	info!("Creating namespace...");
	log_query("create namespace art");
	db.admin_as_root(
		r#"
		create namespace art;
		"#,
		Params::None,
	)
	.unwrap();

	info!("Creating Object enum with structured variants...");
	log_query("CREATE ENUM art::Object { Circle { radius: Float8 }, Rectangle { width: Float8, height: Float8 } }");
	db.admin_as_root(
		r#"
		CREATE ENUM art::Object {
			Circle { radius: Float8 },
			Rectangle { width: Float8, height: Float8 }
		};
		"#,
		Params::None,
	)
	.unwrap();

	info!("Creating drawings table with enum column...");
	log_query("CREATE TABLE art::drawings { id: Int4, shape: art::Object }");
	db.admin_as_root(
		r#"
		CREATE TABLE art::drawings {
			id: Int4,
			shape: art::Object
		};
		"#,
		Params::None,
	)
	.unwrap();

	info!("Inserting drawings with enum values...");
	log_query(
		r#"INSERT art::drawings [
    { id: 1, shape: 'circle', shape_circle_radius: 5.0 },
    { id: 2, shape: 'rectangle', shape_rectangle_width: 3.0, shape_rectangle_height: 4.0 },
    { id: 3, shape: 'circle', shape_circle_radius: 10.0 },
    { id: 4, shape: 'rectangle', shape_rectangle_width: 6.0, shape_rectangle_height: 2.5 }
]"#,
	);
	db.command_as_root(
		r#"
		INSERT art::drawings [
			{ id: 1, shape: 'circle', shape_circle_radius: 5.0 },
			{ id: 2, shape: 'rectangle', shape_rectangle_width: 3.0, shape_rectangle_height: 4.0 },
			{ id: 3, shape: 'circle', shape_circle_radius: 10.0 },
			{ id: 4, shape: 'rectangle', shape_rectangle_width: 6.0, shape_rectangle_height: 2.5 }
		];
		"#,
		Params::None,
	)
	.unwrap();

	info!("Querying all drawings...");
	log_query("FROM art::drawings");
	let results = db
		.query_as_root(
			r#"
			FROM art::drawings
			"#,
			Params::None,
		)
		.unwrap();

	for frame in results {
		info!("{}", frame);
	}

	info!("Filtering for Circle variants...");
	log_query("FROM art::drawings FILTER { shape == 'circle' }");
	let results = db
		.query_as_root(
			r#"
			FROM art::drawings
			FILTER { shape == 'circle' }
			"#,
			Params::None,
		)
		.unwrap();

	for frame in results {
		info!("{}", frame);
	}

	info!("Filtering for Rectangle variants...");
	log_query("FROM art::drawings FILTER { shape == 'rectangle' }");
	let results = db
		.query_as_root(
			r#"
			FROM art::drawings
			FILTER { shape == 'rectangle' }
			"#,
			Params::None,
		)
		.unwrap();

	for frame in results {
		info!("{}", frame);
	}
}
