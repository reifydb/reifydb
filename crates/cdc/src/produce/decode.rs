// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Decode row deltas into columnar `Change` objects.
//!
//! Uses fingerprint-based schema lookup from `SchemaRegistry` to convert
//! `EncodedValues` into `Row`s and then into `Columns`.

use reifydb_catalog::schema::SchemaRegistry;
use reifydb_core::{
	common::CommitVersion,
	encoded::encoded::{EncodedValues, SCHEMA_HEADER_SIZE},
	interface::{
		catalog::primitive::PrimitiveId,
		change::{Change, Diff},
	},
	row::Row,
	value::column::columns::Columns,
};
use reifydb_type::value::row_number::RowNumber;
use tracing::warn;

/// Try to decode an EncodedValues into a Row using the schema registry.
/// Returns None if the values are too short or the schema is not in the cache.
fn decode_row(registry: &SchemaRegistry, row_number: RowNumber, values: EncodedValues) -> Option<Row> {
	if values.len() < SCHEMA_HEADER_SIZE {
		warn!("EncodedValues too short for schema fingerprint ({} < {})", values.len(), SCHEMA_HEADER_SIZE);
		return None;
	}
	let fingerprint = values.fingerprint();
	let schema = registry.get(fingerprint);
	match schema {
		Some(schema) => Some(Row {
			number: row_number,
			encoded: values,
			schema,
		}),
		None => {
			warn!(?fingerprint, "Schema not found in cache for row decode");
			None
		}
	}
}

/// Build an insert Change from a row delta.
pub(crate) fn build_insert_change(
	registry: &SchemaRegistry,
	source: PrimitiveId,
	row_number: RowNumber,
	post: EncodedValues,
	version: CommitVersion,
) -> Option<Change> {
	let row = decode_row(registry, row_number, post)?;
	let columns = Columns::from_row(&row);
	Some(Change::from_primitive(
		source,
		version,
		vec![Diff::Insert {
			post: columns,
		}],
	))
}

/// Build an update Change from a row delta with pre and post values.
pub(crate) fn build_update_change(
	registry: &SchemaRegistry,
	source: PrimitiveId,
	row_number: RowNumber,
	pre: EncodedValues,
	post: EncodedValues,
	version: CommitVersion,
) -> Option<Change> {
	let pre_row = decode_row(registry, row_number, pre)?;
	let post_row = decode_row(registry, row_number, post)?;
	let pre_cols = Columns::from_row(&pre_row);
	let post_cols = Columns::from_row(&post_row);
	Some(Change::from_primitive(
		source,
		version,
		vec![Diff::Update {
			pre: pre_cols,
			post: post_cols,
		}],
	))
}

/// Build a remove Change from a row delta.
pub(crate) fn build_remove_change(
	registry: &SchemaRegistry,
	source: PrimitiveId,
	row_number: RowNumber,
	pre: EncodedValues,
	version: CommitVersion,
) -> Option<Change> {
	let row = decode_row(registry, row_number, pre)?;
	let columns = Columns::from_row(&row);
	Some(Change::from_primitive(
		source,
		version,
		vec![Diff::Remove {
			pre: columns,
		}],
	))
}
