// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

#![cfg_attr(not(debug_assertions), deny(warnings))]

use reifydb::server;
use reifydb_type::params::Params;

fn main() {
	let db = server::memory().build().unwrap();

	println!("--- Schema setup ---");
	let frames = db.admin_as_root("CREATE NAMESPACE test", Params::None).unwrap();
	for frame in &frames {
		println!("{}", frame);
	}

	let frames = db
		.admin_as_root(
			"CREATE ENUM test.Shape { Circle { radius: Float8 }, Rectangle { width: Float8, height: Float8 } }",
			Params::None,
		)
		.unwrap();
	for frame in &frames {
		println!("{}", frame);
	}

	let frames =
		db.admin_as_root("CREATE TABLE test.drawings { id: Int4, shape: test.Shape }", Params::None).unwrap();
	for frame in &frames {
		println!("{}", frame);
	}

	println!("\n--- INSERT ---");
	let result = db.command_as_root(
		r#"INSERT test.drawings [
			{ id: 1, shape: test.Shape::Circle { radius: 5.0 } },
			{ id: 2, shape: test.Shape::Rectangle { width: 3.0, height: 4.0 } },
			{ id: 3, shape: test.Shape::Circle { radius: 10.0 } }
		]"#,
		Params::None,
	);
	match result {
		Ok(frames) => {
			println!("SUCCESS:");
			for frame in &frames {
				println!("{}", frame);
			}
		}
		Err(e) => {
			println!("ERROR: {}", e);
		}
	}

	println!("\n--- FROM test.drawings ---");
	let result = db.command_as_root("FROM test.drawings", Params::None);
	match result {
		Ok(frames) => {
			for frame in &frames {
				println!("{}", frame);
			}
		}
		Err(e) => {
			println!("ERROR: {}", e);
		}
	}
}
