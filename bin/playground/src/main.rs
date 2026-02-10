// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

// #![cfg_attr(not(debug_assertions), deny(warnings))]

use reifydb::server;
use reifydb_type::params::Params;

fn main() {
	let db = server::memory().build().unwrap();

	let frame = db
		.query_as_root(
			r#"

APPEND $data FROM [{id: 1, name: 'Alice'}];
APPEND $data FROM [{id: 2, name: 'Bob'}];
APPEND $data FROM $data | filter { id == 2 };
APPEND $data FROM $data | filter { id == 2 };
APPEND $data FROM $data | filter { id == 1 };
ASSERT { 1 < 1 };
FROM $data;

			"#,
			Params::None,
		)
		.unwrap();

	println!("{}", frame.first().unwrap());
}
