// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

#![cfg_attr(not(debug_assertions), deny(warnings))]

use std::{thread::sleep, time::Duration};

use reifydb::{Params, Session, WithSubsystem, embedded};

fn main() {
	let mut db = embedded::memory().with_worker(|wp| wp).build().unwrap();

	db.start().unwrap();

	// Test EXTEND expressions in scalar contexts
	println!("=== Testing: EXTEND expressions ===");
	for frame in db
		.query_as_root(
			r#"
FROM $env | FILTER key == 'answer' | MAP {answer: cast(value,int1) / 2 }
	"#,
			Params::None,
		)
		.unwrap()
	{
		println!("{}", frame);
	}

	sleep(Duration::from_millis(100));
}
