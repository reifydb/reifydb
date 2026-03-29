// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	encoded::row::EncodedRow,
	interface::catalog::{
		id::NamespaceId,
		sumtype::{SumType, SumTypeKind, Variant},
	},
};
use reifydb_type::value::sumtype::SumTypeId;
use serde_json::from_str;
use tracing::warn;

use self::shape::sumtype;

pub mod create;
pub mod drop;
pub mod find;
pub mod get;
pub mod list;
pub(crate) mod shape;

pub(crate) fn sumtype_from_row(row: &EncodedRow) -> SumType {
	let id = SumTypeId(sumtype::SHAPE.get_u64(row, sumtype::ID));
	let namespace = NamespaceId(sumtype::SHAPE.get_u64(row, sumtype::NAMESPACE));
	let name = sumtype::SHAPE.get_utf8(row, sumtype::NAME).to_string();
	let variants_json = sumtype::SHAPE.get_utf8(row, sumtype::VARIANTS_JSON);
	let variants: Vec<Variant> = from_str(variants_json).unwrap_or_else(|e| {
		warn!("Failed to deserialize sumtype variants for {:?}: {}", id, e);
		vec![]
	});
	let kind = match sumtype::SHAPE.get_u8(row, sumtype::KIND) {
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
