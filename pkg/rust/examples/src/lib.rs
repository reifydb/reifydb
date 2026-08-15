// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
#![cfg_attr(not(debug_assertions), deny(clippy::disallowed_methods))]
#![cfg_attr(debug_assertions, warn(clippy::disallowed_methods))]
#![allow(clippy::tabs_in_doc_comments)]

use reifydb::{Database, Frame, Params, Value};
use tracing::info;

pub fn log_query(query: &str) {
	info!("Query:");
	let formatted_query = query.lines().collect::<Vec<_>>().join("\n");
	info!("{}", formatted_query);
}

/// Runs a statement that may modify state, logging the RQL and every returned frame.
pub fn command(db: &Database, rql: &str) -> Vec<Frame> {
	log_query(rql);
	let frames = db.command_as_root(rql, Params::None).unwrap();
	for frame in &frames {
		info!("{}", frame);
	}
	frames
}

/// Runs a read-only statement, logging the RQL and every returned frame.
pub fn query(db: &Database, rql: &str) -> Vec<Frame> {
	log_query(rql);
	let frames = db.query_as_root(rql, Params::None).unwrap();
	for frame in &frames {
		info!("{}", frame);
	}
	frames
}

/// Every value of one column of the first frame, empty when the statement returned no rows.
pub fn column(frames: &[Frame], name: &str) -> Vec<Value> {
	let Some(frame) = frames.first() else {
		return Vec::new();
	};
	let column = frame.columns.iter().find(|c| c.name == name).unwrap_or_else(|| {
		panic!(
			"result has no column {name}, got {:?}",
			frame.columns.iter().map(|c| &c.name).collect::<Vec<_>>()
		)
	});
	(0..frame.row_count()).map(|i| column.data.get_value(i)).collect()
}

pub fn utf8_column(frames: &[Frame], name: &str) -> Vec<String> {
	column(frames, name)
		.into_iter()
		.map(|v| match v {
			Value::Utf8(s) => s,
			other => panic!("column {name} must be Utf8, got {other:?}"),
		})
		.collect()
}

pub fn uint8_column(frames: &[Frame], name: &str) -> Vec<u64> {
	column(frames, name)
		.into_iter()
		.map(|v| match v {
			Value::Uint8(n) => n,
			other => panic!("column {name} must be Uint8, got {other:?}"),
		})
		.collect()
}

/// Two namespaces covering every exportable object kind except `Queue`, with a dictionary-backed
/// and an enum-typed column on `shop::products` so the examples can show dependency closure.
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
