// Copyright (c) reifydb.com 2025.
// This file is licensed under the AGPL-3.0-or-later, see license.md file.

use reifydb_core::{
	Type, error,
    value::{
		container::{StringContainer, UuidContainer},
    },
};
use reifydb_type::::{BorrowedFragment, LazyFragment};
use reifydb_type::::diagnostic::cast;
use reifydb_type::{
    Uuid4, Uuid7,
    parse::{parse_uuid4, parse_uuid7},
};
use crate::columnar::ColumnData;

pub fn to_uuid<'a>(
	data: &ColumnData,
	target: Type,
	lazy_fragment: impl LazyFragment<'a>,
) -> crate::Result<ColumnData> {
	match data {
		ColumnData::Utf8(container) => {
			from_text(container, target, lazy_fragment)
		}
		ColumnData::Uuid4(container) => {
			from_uuid4(container, target, lazy_fragment)
		}
		ColumnData::Uuid7(container) => {
			from_uuid7(container, target, lazy_fragment)
		}
		_ => {
			let source_type = data.get_type();
			reifydb_core::err!(cast::unsupported_cast(
				lazy_fragment.fragment(),
				source_type,
				target
			))
		}
	}
}

#[inline]
fn from_text<'a>(
	container: &StringContainer,
	target: Type,
	lazy_fragment: impl LazyFragment<'a>,
) -> crate::Result<ColumnData> {
	match target {
		Type::Uuid4 => to_uuid4(container, lazy_fragment),
		Type::Uuid7 => to_uuid7(container, lazy_fragment),
		_ => {
			let source_type = Type::Utf8;
			reifydb_core::err!(cast::unsupported_cast(
				lazy_fragment.fragment(),
				source_type,
				target
			))
		}
	}
}

macro_rules! impl_to_uuid {
    ($fn_name:ident, $type:ty, $target_type:expr, $parse_fn:expr) => {
        #[inline]
        fn $fn_name<'a>(
            container: &StringContainer,
            lazy_fragment: impl LazyFragment<'a>,
        ) -> crate::Result<ColumnData> {
            let mut out = ColumnData::with_capacity($target_type, container.len());
            for idx in 0..container.len() {
                if container.is_defined(idx) {
                    let val = &container[idx];
                    let temp_fragment = BorrowedFragment::new_internal(val.as_str());

                    let parsed = $parse_fn(temp_fragment).map_err(|mut e| {
                        // Get the original fragment for error reporting
                        let proper_fragment = lazy_fragment.fragment().into_owned();

                        // Replace the error's origin with the proper RQL fragment
                        // This ensures the error shows "at col" not the actual value
                        e.0.with_fragment(proper_fragment.clone());

                        // Wrap in cast error with the original fragment
                        error!(cast::invalid_uuid(proper_fragment, $target_type, e.0))
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
fn from_uuid4<'a>(
	container: &UuidContainer<Uuid4>,
	target: Type,
	lazy_fragment: impl LazyFragment<'a>,
) -> crate::Result<ColumnData> {
	match target {
		Type::Uuid4 => Ok(ColumnData::Uuid4(UuidContainer::new(
			container.data().to_vec(),
			container.bitvec().clone(),
		))),
		_ => {
			let source_type = Type::Uuid4;
			reifydb_core::err!(cast::unsupported_cast(
				lazy_fragment.fragment(),
				source_type,
				target
			))
		}
	}
}

#[inline]
fn from_uuid7<'a>(
	container: &UuidContainer<Uuid7>,
	target: Type,
	lazy_fragment: impl LazyFragment<'a>,
) -> crate::Result<ColumnData> {
	match target {
		Type::Uuid7 => Ok(ColumnData::Uuid7(UuidContainer::new(
			container.data().to_vec(),
			container.bitvec().clone(),
		))),
		_ => {
			let source_type = Type::Uuid7;
			reifydb_core::err!(cast::unsupported_cast(
				lazy_fragment.fragment(),
				source_type,
				target
			))
		}
	}
}
