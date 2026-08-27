// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb::{Params, WithSubsystem, embedded};
use reifydb_examples::{command, query, uint8_column, utf8_column};
use tracing::info;

fn main() {
	let db = embedded::memory()
		.with_tracing(|t| t.with_console(|c| c.color(true)).with_filter("info"))
		.build()
		.unwrap();

	db.admin_as_root("CREATE NAMESPACE jobs", Params::None).unwrap();
	db.admin_as_root(
		r#"
		CREATE QUEUE jobs::webhooks {
			endpoint: utf8,
			body: utf8
		} WITH {
			fifo: { partitions: 2, ordered_by: endpoint },
			retry: { attempts: 5, backoff: 10s },
			deduplicate: { by: {endpoint, body}, ttl: 1d }
		};
		"#,
		Params::None,
	)
	.unwrap();

	command(
		&db,
		r#"
		INSERT jobs::webhooks [
			{ endpoint: "billing", body: "invoice.created" },
			{ endpoint: "billing", body: "invoice.paid" },
			{ endpoint: "crm", body: "contact.created" }
		]
		"#,
	);
	command(&db, r#"CALL queue::claim("worker-1", "jobs::webhooks", 1, duration::seconds(30))"#);

	info!("system::queues is the shape of every queue - how it dispatches, retries and dedups...");
	query(&db, r#"FROM system::queues FILTER { name == "webhooks" } MAP { name, partitions, ordered_by }"#);
	query(
		&db,
		r#"FROM system::queues FILTER { name == "webhooks" } MAP { name, deduplicate_by, deduplicate_ttl }"#,
	);

	info!("The same row also carries the live counters an operator watches...");
	info!("depth is waiting work, in_flight is leased work, blocked_keys is work parked behind a key,");
	info!("and oldest_due_at is the age of the backlog - a stalled queue shows it drifting into the past.");
	query(
		&db,
		r#"FROM system::queues FILTER { name == "webhooks" } MAP { depth, in_flight, blocked_keys, oldest_due_at }"#,
	);

	info!("system::queue_partitions breaks the same counters down per partition...");
	info!("An even total hiding one hot partition is invisible in the summary above.");
	let queue_id =
		uint8_column(&query(&db, r#"FROM system::queues FILTER { name == "webhooks" } MAP { id }"#), "id")
			.remove(0);
	query(
		&db,
		&format!(
			"FROM system::queue_partitions filter {{ queue_id == {queue_id} }} map {{ partition, depth, in_flight, blocked_keys }} sort {{ partition:asc }}"
		),
	);

	info!("Both are ordinary tables, so an alert is just a query...");
	query(&db, "FROM system::queues FILTER { depth > 0 } MAP { name, depth, blocked_keys }");

	let stuck = utf8_column(&query(&db, "FROM system::queues FILTER { in_flight > 0 } MAP { name }"), "name");
	info!("queues with work in flight right now: {stuck:?}");
}
