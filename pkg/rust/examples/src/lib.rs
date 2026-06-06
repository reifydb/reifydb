// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB
#![cfg_attr(not(debug_assertions), deny(clippy::disallowed_methods))]
#![cfg_attr(debug_assertions, warn(clippy::disallowed_methods))]
#![allow(clippy::tabs_in_doc_comments)]

use reifydb::{Database, Params};
use tracing::info;

/// Helper function to log queries with formatting
/// The query text is displayed in bold for better readability
pub fn log_query(query: &str) {
	info!("Query:");
	let formatted_query = query.lines().collect::<Vec<_>>().join("\n");
	info!("{}", formatted_query);
}

/// Seed a small demo schema reused across the export examples.
///
/// Creates two namespaces and one of every exportable shape kind, with a
/// dictionary-backed and an enum-typed column on the table, so the examples can
/// demonstrate selection, dependency closure, and round-tripping:
/// - `shop`    : enum `status`, dictionary `tokens`, table `products`
/// - `metrics` : series `events`, ring buffer `recent`
pub fn seed_demo(db: &Database) {
	db.admin_as_root(
		r#"
		create namespace shop;
		create enum shop::status { Active, Inactive };
		create dictionary shop::tokens for utf8 as uint4;
		create table shop::products {
			id: int4,
			name: utf8,
			sym: utf8 with { dictionary: shop::tokens },
			state: shop::status
		};
		create namespace metrics;
		create series metrics::events { ts: datetime, v: int4 } with { key: ts, precision: millisecond };
		create ringbuffer metrics::recent { id: int4, msg: utf8 } with { capacity: 3 };
		"#,
		Params::None,
	)
	.unwrap();

	db.command_as_root(
		r#"
		insert shop::products [
			{ id: 1, name: 'Laptop', sym: 'LPT', state: shop::status::Active },
			{ id: 2, name: 'Mouse', sym: 'MSE', state: shop::status::Active },
			{ id: 3, name: 'Keyboard', sym: 'KBD', state: shop::status::Inactive }
		];
		"#,
		Params::None,
	)
	.unwrap();

	db.command_as_root(
		r#"
		insert metrics::events [
			{ ts: @2024-01-01T00:00:00Z, v: 10 },
			{ ts: @2024-01-01T00:00:01Z, v: 20 },
			{ ts: @2024-01-01T00:00:02Z, v: 30 }
		];
		"#,
		Params::None,
	)
	.unwrap();

	db.command_as_root(
		r#"
		insert metrics::recent [
			{ id: 1, msg: 'a' },
			{ id: 2, msg: 'b' },
			{ id: 3, msg: 'c' },
			{ id: 4, msg: 'd' }
		];
		"#,
		Params::None,
	)
	.unwrap();
}
