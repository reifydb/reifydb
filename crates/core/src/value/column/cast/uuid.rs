// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use arrow_array::{Array, ArrayRef, FixedSizeBinaryArray, LargeStringArray};
use arrow_schema::FieldRef;
use reifydb_value::{
	Result,
	error::{Error, TypeError},
	fragment::{Fragment, LazyFragment},
	value::{
		column_view::{ColumnView, ViewData},
		identity::IdentityId,
		uuid::{
			Uuid4, Uuid7,
			parse::{parse_identity_id, parse_uuid4, parse_uuid7},
		},
		value_type::{
			ValueType,
			field::{FieldType, named},
		},
	},
};

use super::error::CastError;
use crate::value::column::builder::ColumnBuilder;

pub fn to_uuid(data: &ColumnView, target: ValueType, lazy_fragment: impl LazyFragment) -> Result<(FieldRef, ArrayRef)> {
	match &data.data {
		ViewData::Utf8 {
			container,
			..
		} => from_text(container, data.field.name(), target, lazy_fragment),
		ViewData::Uuid4(container) => from_uuid4(data, container, target, lazy_fragment),
		ViewData::Uuid7(container) => from_uuid7(data, container, target, lazy_fragment),
		ViewData::IdentityId(container) => from_identity_id(data, container, target, lazy_fragment),
		_ => {
			let object_type = data.get_type();
			Err(TypeError::UnsupportedCast {
				from: object_type,
				to: target,
				fragment: lazy_fragment.fragment(),
			}
			.into())
		}
	}
}

#[inline]
fn from_text(
	container: &LargeStringArray,
	name: &str,
	target: ValueType,
	lazy_fragment: impl LazyFragment,
) -> Result<(FieldRef, ArrayRef)> {
	match target {
		ValueType::Uuid4 => to_uuid4(container, name, lazy_fragment),
		ValueType::Uuid7 => to_uuid7(container, name, lazy_fragment),
		ValueType::IdentityId => to_identity_id(container, name, lazy_fragment),
		_ => {
			let object_type = ValueType::Utf8;
			Err(TypeError::UnsupportedCast {
				from: object_type,
				to: target,
				fragment: lazy_fragment.fragment(),
			}
			.into())
		}
	}
}

macro_rules! impl_to_uuid {
	($fn_name:ident, $type:ty, $target_type:expr, $parse_fn:expr) => {
		#[inline]
		fn $fn_name(
			container: &LargeStringArray,
			name: &str,
			lazy_fragment: impl LazyFragment,
		) -> Result<(FieldRef, ArrayRef)> {
			let mut out = ColumnBuilder::with_capacity($target_type, container.len());
			for idx in 0..container.len() {
				if container.is_valid(idx) {
					let val = container.value(idx);
					let temp_fragment = Fragment::internal(val);

					let parsed = $parse_fn(temp_fragment).map_err(|mut e| {
						let proper_fragment = lazy_fragment.fragment();

						e.0.with_fragment(proper_fragment.clone());

						Error::from(CastError::InvalidUuid {
							fragment: proper_fragment,
							target: $target_type,
							cause: *e.0,
						})
					})?;

					out.push::<$type>(parsed);
				} else {
					out.push_none();
				}
			}
			Ok(out.finish(name))
		}
	};
}

impl_to_uuid!(to_uuid4, Uuid4, ValueType::Uuid4, parse_uuid4);
impl_to_uuid!(to_uuid7, Uuid7, ValueType::Uuid7, parse_uuid7);
impl_to_uuid!(to_identity_id, IdentityId, ValueType::IdentityId, parse_identity_id);

#[inline]
fn from_uuid4(
	data: &ColumnView,
	container: &FixedSizeBinaryArray,
	target: ValueType,
	lazy_fragment: impl LazyFragment,
) -> Result<(FieldRef, ArrayRef)> {
	match target {
		ValueType::Uuid4 => Ok(retag(data, container, ValueType::Uuid4)),
		_ => {
			let object_type = ValueType::Uuid4;
			Err(TypeError::UnsupportedCast {
				from: object_type,
				to: target,
				fragment: lazy_fragment.fragment(),
			}
			.into())
		}
	}
}

#[inline]
fn from_uuid7(
	data: &ColumnView,
	container: &FixedSizeBinaryArray,
	target: ValueType,
	lazy_fragment: impl LazyFragment,
) -> Result<(FieldRef, ArrayRef)> {
	match target {
		ValueType::Uuid7 => Ok(retag(data, container, ValueType::Uuid7)),
		ValueType::IdentityId => Ok(retag(data, container, ValueType::IdentityId)),
		_ => {
			let object_type = ValueType::Uuid7;
			Err(TypeError::UnsupportedCast {
				from: object_type,
				to: target,
				fragment: lazy_fragment.fragment(),
			}
			.into())
		}
	}
}

#[inline]
fn from_identity_id(
	data: &ColumnView,
	container: &FixedSizeBinaryArray,
	target: ValueType,
	lazy_fragment: impl LazyFragment,
) -> Result<(FieldRef, ArrayRef)> {
	match target {
		ValueType::IdentityId => Ok(retag(data, container, ValueType::IdentityId)),
		ValueType::Uuid7 => Ok(retag(data, container, ValueType::Uuid7)),
		_ => Err(TypeError::UnsupportedCast {
			from: ValueType::IdentityId,
			to: target,
			fragment: lazy_fragment.fragment(),
		}
		.into()),
	}
}

fn retag(data: &ColumnView, container: &FixedSizeBinaryArray, target: ValueType) -> (FieldRef, ArrayRef) {
	let value_type = match data.is_nullable() {
		true => ValueType::Option(Box::new(target)),
		false => target,
	};
	named(
		data.field.name(),
		FieldType {
			value_type: Some(value_type),
			..FieldType::default()
		},
		Arc::new(container.clone()),
	)
}
