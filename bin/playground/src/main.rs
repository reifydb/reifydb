// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{thread::sleep, time::Duration};

use reifydb::{
	Params, Session, SqliteConfig, WithSubsystem,
	core::interface::logging::LogLevel::Info,
	embedded,
	sub_logging::{FormatStyle, LoggingBuilder},
};

fn logger_configuration(logging: LoggingBuilder) -> LoggingBuilder {
	logging.with_console(|console| console.color(true).stderr_for_errors(true).format_style(FormatStyle::Timeline))
		.buffer_capacity(20000)
		.batch_size(2000)
		.flush_interval(Duration::from_millis(50))
		.immediate_on_error(true)
		.level(Info)
}

fn main() {
	// let mut db = embedded::sqlite_optimistic(SqliteConfig::new("/tmp/test/test.db"))
	let mut db = embedded::sqlite_optimistic(SqliteConfig::in_memory())
	// let mut db = embedded::memory_optimistic()
		.with_logging(logger_configuration)
		.with_worker(|wp| wp)
		.with_flow(|f| f)
		.build()
		.unwrap();

	db.start().unwrap();

	db.command_as_root(r#"create namespace test;"#, Params::None).unwrap();
	db.command_as_root(r#"create table test.source { id: int4, name: utf8, age: int4, city: utf8 }"#, Params::None)
		.unwrap();

	let insert_start = std::time::Instant::now();

	// Insert 10 million items in batches of 10,000
	const TOTAL_RECORDS: i32 = 500_000;
	const BATCH_SIZE: i32 = 1_000;
	const NUM_BATCHES: i32 = TOTAL_RECORDS / BATCH_SIZE;

	for batch in 0..NUM_BATCHES {
		let start_id = batch * BATCH_SIZE;
		let mut records = Vec::new();

		for i in 0..BATCH_SIZE {
			let id = start_id + i;
			let name_idx = i % 4;
			let (name, age, city) = match name_idx {
				0 => ("Alice", 30, "NYC"),
				1 => ("Bob", 25, "LA"),
				2 => ("Charlie", 35, "Chicago"),
				_ => ("Diana", 28, "Boston"),
			};
			records.push(format!(r#"{{id: {}, name: "{}", age: {}, city: "{}"}}"#, id, name, age, city));
		}

		let query = format!(r#"from [{}] insert test.source"#, records.join(", "));
		db.command_as_root(&query, Params::None).unwrap();

		if (batch + 1) % 100 == 0 {
			println!("Inserted {} records...", (batch + 1) * BATCH_SIZE);
		}
	}

	let insert_duration = insert_start.elapsed();
	println!("Insertion complete in {:.2}s", insert_duration.as_secs_f64());

	sleep(Duration::from_millis(100));

	println!("\nQuerying first 10 records...");
	let query_start = std::time::Instant::now();

	for frame in db
		.query_as_root(
			r#"
from test.source
take 10
	"#,
			Params::None,
		)
		.unwrap()
	{
		println!("{}", frame);
	}

	let query_duration = query_start.elapsed();
	println!("\nQuery completed in {:.6}s ({:.2}ms)", query_duration.as_secs_f64(), query_duration.as_millis());

	sleep(Duration::from_millis(100));
}
