// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb::embedded;

fn main() {
	let mut db = embedded::memory_serializable().build().unwrap();

	// Start the database
	db.start().unwrap();

	println!("Database started. Press Ctrl+C to stop.");

	// Simple wait loop - in a real application you'd have proper signal
	// handling
	loop {
		std::thread::sleep(std::time::Duration::from_secs(1));
	}
}
