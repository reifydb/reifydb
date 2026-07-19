// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::{
	Result,
	error::{Error, TypeError},
	fragment::{Fragment, LazyFragment},
	value::{
		container::{identity_id::IdentityIdContainer, utf8::Utf8Container, uuid::UuidContainer},
		identity::IdentityId,
		uuid::{
			Uuid4, Uuid7,
			parse::{parse_identity_id, parse_uuid4, parse_uuid7},
		},
		value_type::ValueType,
	},
};

use super::error::CastError;
use crate::value::column::buffer::ColumnBuffer;

pub fn to_uuid(data: &ColumnBuffer, target: ValueType, lazy_fragment: impl LazyFragment) -> Result<ColumnBuffer> {
	match data {
		ColumnBuffer::Utf8 {
			container,
			..
		} => from_text(container, target, lazy_fragment),
		ColumnBuffer::Uuid4(container) => from_uuid4(container, target, lazy_fragment),
		ColumnBuffer::Uuid7(container) => from_uuid7(container, target, lazy_fragment),
		ColumnBuffer::IdentityId(container) => from_identity_id(container, target, lazy_fragment),
		_ => {
			let shape_type = data.get_type();
			Err(TypeError::UnsupportedCast {
				from: shape_type,
				to: target,
				fragment: lazy_fragment.fragment(),
			}
			.into())
		}
	}
}

#[inline]
fn from_text(container: &Utf8Container, target: ValueType, lazy_fragment: impl LazyFragment) -> Result<ColumnBuffer> {
	match target {
		ValueType::Uuid4 => to_uuid4(container, lazy_fragment),
		ValueType::Uuid7 => to_uuid7(container, lazy_fragment),
		ValueType::IdentityId => to_identity_id(container, lazy_fragment),
		_ => {
			let shape_type = ValueType::Utf8;
			Err(TypeError::UnsupportedCast {
				from: shape_type,
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
		fn $fn_name(container: &Utf8Container, lazy_fragment: impl LazyFragment) -> Result<ColumnBuffer> {
			let mut out = ColumnBuffer::with_capacity($target_type, container.len());
			for idx in 0..container.len() {
				if container.is_defined(idx) {
					let val = container.get(idx).unwrap();
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
			Ok(out)
		}
	};
}

impl_to_uuid!(to_uuid4, Uuid4, ValueType::Uuid4, parse_uuid4);
impl_to_uuid!(to_uuid7, Uuid7, ValueType::Uuid7, parse_uuid7);
impl_to_uuid!(to_identity_id, IdentityId, ValueType::IdentityId, parse_identity_id);

#[inline]
fn from_uuid4(
	container: &UuidContainer<Uuid4>,
	target: ValueType,
	lazy_fragment: impl LazyFragment,
) -> Result<ColumnBuffer> {
	match target {
		ValueType::Uuid4 => Ok(ColumnBuffer::Uuid4(UuidContainer::new(container.data().to_vec()))),
		_ => {
			let shape_type = ValueType::Uuid4;
			Err(TypeError::UnsupportedCast {
				from: shape_type,
				to: target,
				fragment: lazy_fragment.fragment(),
			}
			.into())
		}
	}
}

#[inline]
fn from_uuid7(
	container: &UuidContainer<Uuid7>,
	target: ValueType,
	lazy_fragment: impl LazyFragment,
) -> Result<ColumnBuffer> {
	match target {
		ValueType::Uuid7 => Ok(ColumnBuffer::Uuid7(UuidContainer::new(container.data().to_vec()))),
		ValueType::IdentityId => Ok(ColumnBuffer::IdentityId(IdentityIdContainer::from_vec(
			container.data().iter().map(|u| IdentityId(*u)).collect(),
		))),
		_ => {
			let shape_type = ValueType::Uuid7;
			Err(TypeError::UnsupportedCast {
				from: shape_type,
				to: target,
				fragment: lazy_fragment.fragment(),
			}
			.into())
		}
	}
}

#[inline]
fn from_identity_id(
	container: &IdentityIdContainer,
	target: ValueType,
	lazy_fragment: impl LazyFragment,
) -> Result<ColumnBuffer> {
	match target {
		ValueType::IdentityId => {
			Ok(ColumnBuffer::IdentityId(IdentityIdContainer::from_vec(container.data().to_vec())))
		}
		ValueType::Uuid7 => {
			Ok(ColumnBuffer::Uuid7(UuidContainer::new(container.data().iter().map(|id| id.0).collect())))
		}
		_ => Err(TypeError::UnsupportedCast {
			from: ValueType::IdentityId,
			to: target,
			fragment: lazy_fragment.fragment(),
		}
		.into()),
	}
}
