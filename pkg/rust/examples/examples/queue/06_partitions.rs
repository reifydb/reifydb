// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb::{Database, Params, WithSubsystem, embedded};
use reifydb_examples::{command, query, uint8_column, utf8_column};
use tracing::info;

fn main() {
	let db = embedded::memory()
		.with_tracing(|t| t.with_console(|c| c.color(true)).with_filter("info"))
		.build()
		.unwrap();

	db.admin_as_root("CREATE NAMESPACE jobs", Params::None).unwrap();

	info!("partitions is how many workers can claim at once without contending on the same lock...");
	db.admin_as_root(
		"CREATE QUEUE jobs::thumbs { image: utf8 } WITH { fifo: { partitions: 4 } }",
		Params::None,
	)
	.unwrap();

	let rows: Vec<String> = (1..=12).map(|i| format!(r#"{{ image: "photo-{i}.jpg" }}"#)).collect();
	command(&db, &format!("INSERT jobs::thumbs [{}]", rows.join(", ")));

	let queue_id = uint8_column(&query(&db, r#"FROM system::queues FILTER { name == "thumbs" } MAP { id }"#), "id")
		.remove(0);

	info!("Items are spread across partitions on insert, so no single partition holds them all...");
	partitions(&db, queue_id);

	info!("Two workers claim side by side - each takes whole partitions, never the same item...");
	let first = command(&db, r#"CALL queue::claim("worker-1", "jobs::thumbs", 4, duration::seconds(30))"#);
	let second = command(&db, r#"CALL queue::claim("worker-2", "jobs::thumbs", 4, duration::seconds(30))"#);

	let mine = uint8_column(&first, "item");
	let theirs = uint8_column(&second, "item");
	let overlap: Vec<u64> = mine.iter().copied().filter(|item| theirs.contains(item)).collect();
	info!("worker-1 got items {mine:?}, worker-2 got {theirs:?}, overlap {overlap:?}");
	assert!(overlap.is_empty(), "a live lease must never hand the same item to two workers");

	info!("in_flight is now spread over the partitions the two workers drew from...");
	partitions(&db, queue_id);

	for token in utf8_column(&first, "token").into_iter().chain(utf8_column(&second, "token")) {
		command(&db, &format!(r#"CALL queue::ack("{token}", "ok", none)"#));
	}
}

fn partitions(db: &Database, queue_id: u64) {
	query(
		db,
		&format!(
			"FROM system::queue_partitions filter {{ queue_id == {queue_id} }} map {{ partition, depth, in_flight }} sort {{ partition:asc }}"
		),
	);
}
