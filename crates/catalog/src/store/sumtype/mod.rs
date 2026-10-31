// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::catalog::EncodedCatalogRow;
use reifydb_core::interface::catalog::{
	id::NamespaceId,
	sumtype::{SumType, SumTypeKind, Variant},
};
use reifydb_value::value::sumtype::SumTypeId;
use serde_json::from_str;

use self::shape::sumtype;
use crate::{Result, error::CatalogError};

pub mod create;
pub mod drop;
pub mod find;
pub mod get;
pub mod list;
pub(crate) mod shape;

pub(crate) fn sumtype_from_bytes(bytes: &EncodedCatalogRow) -> Result<SumType> {
	let id = SumTypeId(sumtype::get_id(bytes));
	let namespace = NamespaceId(sumtype::get_namespace(bytes));
	let name = sumtype::get_name(bytes).to_string();
	let variants: Vec<Variant> =
		from_str(sumtype::get_variants_json(bytes)).map_err(|e| CatalogError::SumTypeCorrupt {
			id,
			reason: e.to_string(),
		})?;
	let kind = match sumtype::get_kind(bytes) {
		0 => SumTypeKind::Enum,
		1 => SumTypeKind::Event,
		other => {
			return Err(CatalogError::SumTypeCorrupt {
				id,
				reason: format!("unknown kind {}", other),
			}
			.into());
		}
	};

	Ok(SumType {
		id,
		namespace,
		name,
		variants,
		kind,
	})
}

#[cfg(test)]
mod tests {
	use super::sumtype_from_bytes;
	use crate::store::sumtype::shape::sumtype;

	#[test]
	fn a_sumtype_row_with_an_unknown_kind_is_corrupt() {
		// Reading an unknown kind as an enum would run filters and writes against a type the row never
		// declared.
		let mut row = sumtype::allocate();
		sumtype::set_name(&mut row, "phantom");
		sumtype::set_variants_json(&mut row, "[]");
		sumtype::set_kind(&mut row, 2);
		let diagnostic = sumtype_from_bytes(&row.freeze()).unwrap_err().diagnostic();
		assert_eq!(diagnostic.code, "CA_135");
		assert!(diagnostic.message.contains("unknown kind 2"), "{}", diagnostic.message);
	}

	#[test]
	fn a_sumtype_row_with_unreadable_variants_is_corrupt() {
		// An empty variant list would turn every name into a not-a-variant error and hide the broken row.
		let mut row = sumtype::allocate();
		sumtype::set_name(&mut row, "phantom");
		sumtype::set_variants_json(&mut row, "not json");
		sumtype::set_kind(&mut row, 0);
		let diagnostic = sumtype_from_bytes(&row.freeze()).unwrap_err().diagnostic();
		assert_eq!(diagnostic.code, "CA_135");
	}
}
