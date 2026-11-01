// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{common::TimeSource, interface::catalog::series::SeriesEncoding};
use reifydb_value::{fragment::Fragment, value::sumtype::SumTypeId};

use crate::{
	Result,
	error::{CatalogError, CatalogObjectKind},
};

fn invalid(kind: CatalogObjectKind, name: &Fragment, field: &'static str, reason: &str) -> CatalogError {
	CatalogError::InvalidDefinition {
		kind,
		name: name.text().to_string(),
		field,
		reason: reason.to_string(),
		fragment: name.clone(),
	}
}

pub(crate) fn reject_invalid_column_list(
	kind: CatalogObjectKind,
	name: &Fragment,
	field: &'static str,
	columns: &[String],
) -> Result<()> {
	if columns.iter().any(String::is_empty) {
		return Err(invalid(kind, name, field, "must not contain an empty column name").into());
	}
	if columns.iter().any(|column| column.contains(',')) {
		return Err(invalid(kind, name, field, "must not contain a column name with ','").into());
	}
	Ok(())
}

pub(crate) fn reject_invalid_time(kind: CatalogObjectKind, name: &Fragment, time: &TimeSource) -> Result<()> {
	if matches!(time, TimeSource::Event { ts } if ts.is_empty()) {
		return Err(invalid(kind, name, "time", "must name a timestamp column").into());
	}
	Ok(())
}

pub(crate) fn reject_zero_tag(kind: CatalogObjectKind, name: &Fragment, tag: Option<SumTypeId>) -> Result<()> {
	if tag.is_some_and(|tag| tag.to_u64() == 0) {
		return Err(invalid(kind, name, "tag", "must not be 0").into());
	}
	Ok(())
}

pub(crate) fn reject_invalid_encoding(name: &Fragment, encoding: Option<&SeriesEncoding>) -> Result<()> {
	let Some(encoding) = encoding else {
		return Ok(());
	};
	if encoding.name.is_empty() {
		return Err(invalid(CatalogObjectKind::Series, name, "encoding", "must have a name").into());
	}
	if encoding.config.is_empty() {
		return Err(invalid(CatalogObjectKind::Series, name, "encoding", "must have config bytes").into());
	}
	Ok(())
}

pub(crate) fn reject_empty_text(
	kind: CatalogObjectKind,
	name: &Fragment,
	field: &'static str,
	value: Option<&str>,
) -> Result<()> {
	if value.is_some_and(str::is_empty) {
		return Err(invalid(kind, name, field, "must not be empty").into());
	}
	Ok(())
}
