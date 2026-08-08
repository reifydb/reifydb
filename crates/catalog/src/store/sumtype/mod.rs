// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::bytes::EncodedBytes;
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

pub(crate) fn sumtype_from_bytes(bytes: &EncodedBytes) -> SumType {
	let id = SumTypeId(sumtype::SHAPE.get::<u64>(bytes, sumtype::ID));
	let namespace = NamespaceId(sumtype::SHAPE.get::<u64>(bytes, sumtype::NAMESPACE));
	let name = sumtype::SHAPE.get_utf8(bytes, sumtype::NAME).to_string();
	let variants_json = sumtype::SHAPE.get_utf8(bytes, sumtype::VARIANTS_JSON);
	let variants: Vec<Variant> = from_str(variants_json).unwrap_or_else(|e| {
		warn!("Failed to deserialize sumtype variants for {:?}: {}", id, e);
		vec![]
	});
	let kind = match sumtype::SHAPE.get::<u8>(bytes, sumtype::KIND) {
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
