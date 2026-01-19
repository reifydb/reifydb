// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Cast to UUID types

use reifydb_core::value::column::data::ColumnData;
use reifydb_type::{
	fragment::Fragment,
	value::{
		container::{utf8::Utf8Container, uuid::UuidContainer},
		r#type::Type,
		uuid::{
			Uuid4, Uuid7,
			parse::{parse_uuid4, parse_uuid7},
		},
	},
};

use crate::expression::types::{EvalError, EvalResult};

pub(super) fn to_uuid(data: &ColumnData, target: Type) -> EvalResult<ColumnData> {
	match data {
		ColumnData::Utf8 { container, .. } => from_text(container, target),
		ColumnData::Uuid4(container) => from_uuid4(container, target),
		ColumnData::Uuid7(container) => from_uuid7(container, target),
		_ => {
			let source_type = data.get_type();
			Err(EvalError::UnsupportedCast {
				from: format!("{:?}", source_type),
				to: format!("{:?}", target),
			})
		}
	}
}

#[inline]
fn from_text(container: &Utf8Container, target: Type) -> EvalResult<ColumnData> {
	match target {
		Type::Uuid4 => to_uuid4(container),
		Type::Uuid7 => to_uuid7(container),
		_ => Err(EvalError::UnsupportedCast {
			from: "Utf8".to_string(),
			to: format!("{:?}", target),
		}),
	}
}

macro_rules! impl_to_uuid {
	($fn_name:ident, $type:ty, $target_type:expr, $parse_fn:expr) => {
		#[inline]
		fn $fn_name(container: &Utf8Container) -> EvalResult<ColumnData> {
			let mut out = ColumnData::with_capacity($target_type, container.len());
			for idx in 0..container.len() {
				if container.is_defined(idx) {
					let val = &container[idx];
					let temp_fragment = Fragment::internal(val.as_str());

					let parsed = $parse_fn(temp_fragment).map_err(|_e| EvalError::InvalidCast {
						details: format!("Cannot parse '{}' as {:?}", val, $target_type),
					})?;

					out.push::<$type>(parsed);
				} else {
					out.push_undefined();
				}
			}
			Ok(out)
		}
	};
}

impl_to_uuid!(to_uuid4, Uuid4, Type::Uuid4, parse_uuid4);
impl_to_uuid!(to_uuid7, Uuid7, Type::Uuid7, parse_uuid7);

#[inline]
fn from_uuid4(container: &UuidContainer<Uuid4>, target: Type) -> EvalResult<ColumnData> {
	match target {
		Type::Uuid4 => Ok(ColumnData::Uuid4(UuidContainer::new(
			container.data().to_vec(),
			container.bitvec().clone(),
		))),
		_ => Err(EvalError::UnsupportedCast {
			from: "Uuid4".to_string(),
			to: format!("{:?}", target),
		}),
	}
}

#[inline]
fn from_uuid7(container: &UuidContainer<Uuid7>, target: Type) -> EvalResult<ColumnData> {
	match target {
		Type::Uuid7 => Ok(ColumnData::Uuid7(UuidContainer::new(
			container.data().to_vec(),
			container.bitvec().clone(),
		))),
		_ => Err(EvalError::UnsupportedCast {
			from: "Uuid7".to_string(),
			to: format!("{:?}", target),
		}),
	}
}
