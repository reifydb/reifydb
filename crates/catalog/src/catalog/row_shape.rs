// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_abi::data::constraint::ExternCTypeConstraint;
use reifydb_codec::{
	constraint::type_constraint_from_extern_c,
	row::{
		pod::EncodedPodRow,
		shape::{RowFamily, RowShape, RowShapeField, fingerprint::RowShapeFingerprint},
	},
};
use reifydb_core::{
	error::diagnostic::internal::internal,
	key::row_shape::{RowShapeFieldKey, RowShapeKey},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::error::Error;
use tracing::{Span, field, instrument};

use crate::{
	Result,
	catalog::Catalog,
	store::row_shape::{
		create::create_row_shape,
		find::find_row_shape_by_fingerprint,
		shape::{shape_field, shape_header},
	},
};

impl Catalog {
	#[instrument(
		name = "catalog::row_shape::get_or_create",
		level = "debug",
		skip(self, txn, fields),
		fields(fingerprint = field::Empty, field_count = fields.len())
	)]
	pub fn get_or_create_row_shape(
		&self,
		txn: &mut Transaction<'_>,
		family: RowFamily,
		fields: Vec<RowShapeField>,
	) -> Result<RowShape> {
		let shape = RowShape::new(family, fields);
		let fingerprint = shape.fingerprint();
		Span::current().record("fingerprint", field::debug(&fingerprint));

		if let Some(cached) = self.cache.find_row_shape(fingerprint) {
			return Ok(cached);
		}

		if let Some(stored_shape) = find_row_shape_by_fingerprint(txn, fingerprint)? {
			self.cache.set_row_shape(stored_shape.clone());
			return Ok(stored_shape);
		}

		create_row_shape(txn, &shape)?;

		self.cache.set_row_shape(shape.clone());

		Ok(shape)
	}

	#[instrument(
		name = "catalog::row_shape::get_or_load",
		level = "debug",
		skip(self, txn),
		fields(
			fingerprint = ?fingerprint,
			cache_hit = field::Empty,
			field_count = field::Empty
		)
	)]
	pub fn get_or_load_row_shape(
		&self,
		fingerprint: RowShapeFingerprint,
		txn: &mut Transaction<'_>,
	) -> Result<Option<RowShape>> {
		if let Some(shape) = self.cache.find_row_shape(fingerprint) {
			Span::current().record("cache_hit", true);
			Span::current().record("field_count", shape.field_count());
			return Ok(Some(shape));
		}

		let header_key = RowShapeKey::encoded(fingerprint);
		let header_entry = match txn.get(&header_key)? {
			Some(entry) => entry,
			None => {
				Span::current().record("cache_hit", false);
				Span::current().record("field_count", 0);
				return Ok(None);
			}
		};

		let (family, field_count) = shape_header::decode(EncodedPodRow::view(&header_entry.bytes))?;
		let field_count = field_count as usize;

		let mut fields = Vec::with_capacity(field_count);
		for i in 0..field_count {
			let field_key = RowShapeFieldKey::encoded(fingerprint, i as u16);
			let field_entry = txn.get(&field_key)?.ok_or_else(|| {
				Error(Box::new(internal(format!(
					"RowShape field {} missing for fingerprint {:?}",
					i, fingerprint
				))))
			})?;

			let name = shape_field::SHAPE.get_utf8(&field_entry.bytes, shape_field::NAME).to_string();
			let base_type = shape_field::SHAPE.get::<u8>(&field_entry.bytes, shape_field::TYPE);
			let constraint_type =
				shape_field::SHAPE.get::<u8>(&field_entry.bytes, shape_field::CONSTRAINT_TYPE);
			let constraint_param1 =
				shape_field::SHAPE.get::<u32>(&field_entry.bytes, shape_field::CONSTRAINT_P1);
			let constraint_param2 =
				shape_field::SHAPE.get::<u32>(&field_entry.bytes, shape_field::CONSTRAINT_P2);
			let constraint = type_constraint_from_extern_c(&ExternCTypeConstraint {
				base_type,
				constraint_type,
				constraint_param1,
				constraint_param2,
			})
			.expect("invalid persisted type constraint tag");
			let offset = shape_field::SHAPE.get::<u32>(&field_entry.bytes, shape_field::OFFSET);
			let size = shape_field::SHAPE.get::<u32>(&field_entry.bytes, shape_field::SIZE);

			fields.push(RowShapeField {
				name,
				constraint,
				offset,
				size,
			});
		}

		let shape = RowShape::from_parts(family, fingerprint, fields);
		Span::current().record("cache_hit", false);
		Span::current().record("field_count", shape.field_count());
		self.cache.set_row_shape(shape.clone());

		Ok(Some(shape))
	}

	pub fn find_row_shape(&self, fingerprint: RowShapeFingerprint) -> Option<RowShape> {
		self.cache.find_row_shape(fingerprint)
	}
}
