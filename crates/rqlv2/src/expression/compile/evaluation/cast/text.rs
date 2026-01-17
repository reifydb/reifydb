// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Cast to UTF8/Text type

use std::fmt::Display;

use reifydb_core::value::column::data::ColumnData;
use reifydb_type::value::{
	container::{bool::BoolContainer, number::NumberContainer, temporal::TemporalContainer, uuid::UuidContainer},
	is::{IsNumber, IsTemporal, IsUuid},
	r#type::Type,
};

use crate::expression::types::{EvalError, EvalResult};

pub(super) fn to_text(data: &ColumnData) -> EvalResult<ColumnData> {
	match data {
		ColumnData::Blob {
			container,
			..
		} => from_blob(container),
		ColumnData::Bool(container) => from_bool(container),
		ColumnData::Int1(container) => from_number(container),
		ColumnData::Int2(container) => from_number(container),
		ColumnData::Int4(container) => from_number(container),
		ColumnData::Int8(container) => from_number(container),
		ColumnData::Int16(container) => from_number(container),
		ColumnData::Uint1(container) => from_number(container),
		ColumnData::Uint2(container) => from_number(container),
		ColumnData::Uint4(container) => from_number(container),
		ColumnData::Uint8(container) => from_number(container),
		ColumnData::Uint16(container) => from_number(container),
		ColumnData::Float4(container) => from_number(container),
		ColumnData::Float8(container) => from_number(container),
		ColumnData::Date(container) => from_temporal(container),
		ColumnData::DateTime(container) => from_temporal(container),
		ColumnData::Time(container) => from_temporal(container),
		ColumnData::Duration(container) => from_temporal(container),
		ColumnData::Uuid4(container) => from_uuid(container),
		ColumnData::Uuid7(container) => from_uuid(container),
		_ => {
			let source_type = data.get_type();
			Err(EvalError::UnsupportedCast {
				from: format!("{:?}", source_type),
				to: "Utf8".to_string(),
			})
		}
	}
}

#[inline]
fn from_blob(container: &reifydb_type::value::container::blob::BlobContainer) -> EvalResult<ColumnData> {
	let mut out = ColumnData::with_capacity(Type::Utf8, container.len());
	for idx in 0..container.len() {
		if container.is_defined(idx) {
			match container[idx].to_utf8() {
				Ok(s) => out.push(s),
				Err(_e) => {
					return Err(EvalError::InvalidCast {
						details: "Invalid UTF-8 in blob".to_string(),
					});
				}
			}
		} else {
			out.push_undefined()
		}
	}
	Ok(out)
}

#[inline]
fn from_bool(container: &BoolContainer) -> EvalResult<ColumnData> {
	let mut out = ColumnData::with_capacity(Type::Utf8, container.len());
	for idx in 0..container.len() {
		if container.is_defined(idx) {
			out.push::<String>(container.data().get(idx).to_string());
		} else {
			out.push_undefined();
		}
	}
	Ok(out)
}

#[inline]
fn from_number<T>(container: &NumberContainer<T>) -> EvalResult<ColumnData>
where
	T: Copy + Display + IsNumber + Default,
{
	let mut out = ColumnData::with_capacity(Type::Utf8, container.len());
	for idx in 0..container.len() {
		if container.is_defined(idx) {
			out.push::<String>(container[idx].to_string());
		} else {
			out.push_undefined();
		}
	}
	Ok(out)
}

#[inline]
fn from_temporal<T>(container: &TemporalContainer<T>) -> EvalResult<ColumnData>
where
	T: Copy + Display + IsTemporal + Default,
{
	let mut out = ColumnData::with_capacity(Type::Utf8, container.len());
	for idx in 0..container.len() {
		if container.is_defined(idx) {
			out.push::<String>(container[idx].to_string());
		} else {
			out.push_undefined();
		}
	}
	Ok(out)
}

#[inline]
fn from_uuid<T>(container: &UuidContainer<T>) -> EvalResult<ColumnData>
where
	T: Copy + Display + IsUuid + Default,
{
	let mut out = ColumnData::with_capacity(Type::Utf8, container.len());
	for idx in 0..container.len() {
		if container.is_defined(idx) {
			out.push::<String>(container[idx].to_string());
		} else {
			out.push_undefined();
		}
	}
	Ok(out)
}
