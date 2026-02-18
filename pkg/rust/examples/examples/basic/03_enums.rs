// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! # Basic Enums Example
//!
//! Demonstrates enum (sum type / tagged union) support in ReifyDB:
//! - Creating enums with structured variants
//! - Using enums as column types in tables
//! - Inserting rows with enum values
//! - Querying tables containing enum data
//! - Filtering rows by enum variant
//!
//! Run with: `make basic-enums` or `cargo run --bin basic-enums`

use reifydb::{Params, embedded};
use reifydb_examples::log_query;
use tracing::info;

fn main() {
	let mut db = embedded::memory().build().unwrap();
	db.start().unwrap();

	info!("Creating namespace...");
	log_query("create namespace art");
	db.admin_as_root(
		r#"
		create namespace art;
		"#,
		Params::None,
	)
	.unwrap();

	info!("Creating Shape enum with structured variants...");
	log_query("CREATE ENUM art.Shape { Circle { radius: Float8 }, Rectangle { width: Float8, height: Float8 } }");
	db.admin_as_root(
		r#"
		CREATE ENUM art.Shape {
			Circle { radius: Float8 },
			Rectangle { width: Float8, height: Float8 }
		};
		"#,
		Params::None,
	)
	.unwrap();

	info!("Creating drawings table with enum column...");
	log_query("CREATE TABLE art.drawings { id: Int4, shape: art.Shape }");
	db.admin_as_root(
		r#"
		CREATE TABLE art.drawings {
			id: Int4,
			shape: art.Shape
		};
		"#,
		Params::None,
	)
	.unwrap();

	info!("Inserting drawings with enum values...");
	log_query(
		r#"INSERT art.drawings [
    { id: 1, shape: art.Shape::Circle { radius: 5.0 } },
    { id: 2, shape: art.Shape::Rectangle { width: 3.0, height: 4.0 } },
    { id: 3, shape: art.Shape::Circle { radius: 10.0 } },
    { id: 4, shape: art.Shape::Rectangle { width: 6.0, height: 2.5 } }
]"#,
	);
	db.command_as_root(
		r#"
		INSERT art.drawings [
			{ id: 1, shape: art.Shape::Circle { radius: 5.0 } },
			{ id: 2, shape: art.Shape::Rectangle { width: 3.0, height: 4.0 } },
			{ id: 3, shape: art.Shape::Circle { radius: 10.0 } },
			{ id: 4, shape: art.Shape::Rectangle { width: 6.0, height: 2.5 } }
		];
		"#,
		Params::None,
	)
	.unwrap();

	info!("Querying all drawings...");
	log_query("FROM art.drawings");
	let results = db
		.query_as_root(
			r#"
			FROM art.drawings
			"#,
			Params::None,
		)
		.unwrap();

	for frame in results {
		info!("{}", frame);
	}

	info!("Filtering for Circle variants...");
	log_query("FROM art.drawings FILTER { shape IS art.Shape::Circle }");
	let results = db
		.query_as_root(
			r#"
			FROM art.drawings
			FILTER { shape IS art.Shape::Circle }
			"#,
			Params::None,
		)
		.unwrap();

	for frame in results {
		info!("{}", frame);
	}

	info!("Filtering for Rectangle variants...");
	log_query("FROM art.drawings FILTER { shape IS art.Shape::Rectangle }");
	let results = db
		.query_as_root(
			r#"
			FROM art.drawings
			FILTER { shape IS art.Shape::Rectangle }
			"#,
			Params::None,
		)
		.unwrap();

	for frame in results {
		info!("{}", frame);
	}
}
