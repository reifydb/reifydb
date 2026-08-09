// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::catalog::EncodedCatalogRow;
use reifydb_core::interface::catalog::{
	id::NamespaceId,
	sumtype::{SumType, SumTypeKind, Variant},
};
use reifydb_value::value::sumtype::SumTypeId;
use serde_json::from_str;
use tracing::warn;

use self::shape::sumtype;

pub mod create;
pub mod drop;
pub mod find;
pub mod get;
pub mod list;
pub(crate) mod shape;

pub(crate) fn sumtype_from_bytes(bytes: &EncodedCatalogRow) -> SumType {
	let id = SumTypeId(sumtype::get_id(bytes));
	let namespace = NamespaceId(sumtype::get_namespace(bytes));
	let name = sumtype::get_name(bytes).to_string();
	let variants_json = sumtype::get_variants_json(bytes);
	let variants: Vec<Variant> = from_str(variants_json).unwrap_or_else(|e| {
		warn!("Failed to deserialize sumtype variants for {:?}: {}", id, e);
		vec![]
	});
	let kind = match sumtype::get_kind(bytes) {
		0 => SumTypeKind::Enum,
		1 => SumTypeKind::Event,
		2 => SumTypeKind::Tag,
		other => {
			warn!("Unknown SumTypeKind discriminant {} for {:?}, defaulting to Enum", other, id);
			SumTypeKind::Enum
		}
	};

	SumType {
		id,
		namespace,
		name,
		variants,
		kind,
	}
}
