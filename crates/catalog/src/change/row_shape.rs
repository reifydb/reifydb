// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_abi::data::constraint::FFITypeConstraint;
use reifydb_codec::{
	constraint::type_constraint_from_ffi,
	key::encoded::EncodedKey,
	row::{
		bytes::EncodedBytes,
		shape::{RowShape, RowShapeField, fingerprint::RowShapeFingerprint},
	},
};
use reifydb_core::key::{
	EncodableKey,
	kind::KeyKind,
	row_shape::{RowShapeFieldKey, RowShapeKey},
};
use reifydb_transaction::transaction::Transaction;

use super::CatalogChangeApplier;
use crate::{
	Result,
	catalog::Catalog,
	error::CatalogChangeError,
	store::row_shape::shape::{shape_field, shape_header},
};

pub(super) struct RowShapeHeaderApplier;

impl CatalogChangeApplier for RowShapeHeaderApplier {
	fn set(catalog: &Catalog, txn: &mut Transaction<'_>, key: &EncodedKey, bytes: &EncodedBytes) -> Result<()> {
		txn.set(key, bytes.clone())?;

		let shape_key = RowShapeKey::decode(key).ok_or(CatalogChangeError::KeyDecodeFailed {
			kind: KeyKind::RowShape,
		})?;
		let field_count = shape_header::SHAPE.get::<u16>(bytes, shape_header::FIELD_COUNT);

		try_reconstruct(catalog, txn, shape_key.fingerprint, field_count)
	}

	fn remove(_catalog: &Catalog, txn: &mut Transaction<'_>, key: &EncodedKey) -> Result<()> {
		txn.remove(key)
	}
}

pub(super) struct RowShapeFieldApplier;

impl CatalogChangeApplier for RowShapeFieldApplier {
	fn set(catalog: &Catalog, txn: &mut Transaction<'_>, key: &EncodedKey, bytes: &EncodedBytes) -> Result<()> {
		txn.set(key, bytes.clone())?;

		let field_key = RowShapeFieldKey::decode(key).ok_or(CatalogChangeError::KeyDecodeFailed {
			kind: KeyKind::RowShapeField,
		})?;
		let fingerprint = field_key.shape_fingerprint;

		let header_key = RowShapeKey::encoded(fingerprint);
		let header_entry = match txn.get(&header_key)? {
			Some(entry) => entry,
			None => return Ok(()),
		};
		let field_count = shape_header::SHAPE.get::<u16>(&header_entry.bytes, shape_header::FIELD_COUNT);

		try_reconstruct(catalog, txn, fingerprint, field_count)
	}

	fn remove(_catalog: &Catalog, txn: &mut Transaction<'_>, key: &EncodedKey) -> Result<()> {
		txn.remove(key)
	}
}

fn try_reconstruct(
	catalog: &Catalog,
	txn: &mut Transaction<'_>,
	fingerprint: RowShapeFingerprint,
	field_count: u16,
) -> Result<()> {
	if catalog.cache.find_row_shape(fingerprint).is_some() {
		return Ok(());
	}

	let mut fields = Vec::with_capacity(field_count as usize);
	for i in 0..field_count {
		let field_key = RowShapeFieldKey::encoded(fingerprint, i);
		match txn.get(&field_key)? {
			Some(entry) => {
				let bytes = &entry.bytes;
				let name = shape_field::SHAPE.get_utf8(bytes, shape_field::NAME).to_string();
				let base_type = shape_field::SHAPE.get::<u8>(bytes, shape_field::TYPE);
				let constraint_type = shape_field::SHAPE.get::<u8>(bytes, shape_field::CONSTRAINT_TYPE);
				let constraint_param1 =
					shape_field::SHAPE.get::<u32>(bytes, shape_field::CONSTRAINT_P1);
				let constraint_param2 =
					shape_field::SHAPE.get::<u32>(bytes, shape_field::CONSTRAINT_P2);
				let constraint = type_constraint_from_ffi(&FFITypeConstraint {
					base_type,
					constraint_type,
					constraint_param1,
					constraint_param2,
				})
				.expect("invalid persisted type constraint tag");
				let offset = shape_field::SHAPE.get::<u32>(bytes, shape_field::OFFSET);
				let size = shape_field::SHAPE.get::<u32>(bytes, shape_field::SIZE);
				let align = shape_field::SHAPE.get::<u8>(bytes, shape_field::ALIGN);

				fields.push(RowShapeField {
					name,
					constraint,
					offset,
					size,
					align,
				});
			}
			None => return Ok(()),
		}
	}

	let shape = RowShape::from_parts(fingerprint, fields);
	catalog.cache.set_row_shape(shape);
	Ok(())
}
