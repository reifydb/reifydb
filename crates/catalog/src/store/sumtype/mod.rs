// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	encoded::encoded::EncodedValues,
	interface::catalog::{
		id::NamespaceId,
		sumtype::{SumTypeDef, SumTypeKind, VariantDef},
	},
};
use reifydb_type::value::sumtype::SumTypeId;
use serde_json::from_str;
use tracing::warn;

use self::schema::sumtype;

pub mod create;
pub mod drop;
pub mod find;
pub mod get;
pub mod list;
pub(crate) mod schema;

pub(crate) fn sumtype_def_from_row(row: &EncodedValues) -> SumTypeDef {
	let id = SumTypeId(sumtype::SCHEMA.get_u64(row, sumtype::ID));
	let namespace = NamespaceId(sumtype::SCHEMA.get_u64(row, sumtype::NAMESPACE));
	let name = sumtype::SCHEMA.get_utf8(row, sumtype::NAME).to_string();
	let variants_json = sumtype::SCHEMA.get_utf8(row, sumtype::VARIANTS_JSON);
	let variants: Vec<VariantDef> = from_str(variants_json).unwrap_or_else(|e| {
		warn!("Failed to deserialize sumtype variants for {:?}: {}", id, e);
		vec![]
	});
	let kind = match sumtype::SCHEMA.get_u8(row, sumtype::KIND) {
		0 => SumTypeKind::Enum,
		1 => SumTypeKind::Event,
		2 => SumTypeKind::Tag,
		other => {
			warn!("Unknown SumTypeKind discriminant {} for {:?}, defaulting to Enum", other, id);
			SumTypeKind::Enum
		}
	};

	SumTypeDef {
		id,
		namespace,
		name,
		variants,
		kind,
	}
}
