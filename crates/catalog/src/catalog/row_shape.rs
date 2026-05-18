// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	encoded::shape::{RowShape, RowShapeField, fingerprint::RowShapeFingerprint},
	error::diagnostic::internal::internal,
	key::shape::{RowShapeFieldKey, RowShapeKey},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{
	error::Error,
	value::constraint::{FFITypeConstraint, TypeConstraint},
};
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
		fields: Vec<RowShapeField>,
	) -> Result<RowShape> {
		let shape = RowShape::new(fields);
		let fingerprint = shape.fingerprint();
		Span::current().record("fingerprint", field::debug(&fingerprint));

		if let Some(cached) = self.cache.find_row_shape(fingerprint) {
			return Ok(cached);
		}

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

		let field_count = shape_header::SHAPE.get_u16(&header_entry.row, shape_header::FIELD_COUNT) as usize;

		let mut fields = Vec::with_capacity(field_count);
		for i in 0..field_count {
			let field_key = RowShapeFieldKey::encoded(fingerprint, i as u16);
			let field_entry = txn.get(&field_key)?.ok_or_else(|| {
				Error(Box::new(internal(format!(
					"RowShape field {} missing for fingerprint {:?}",
					i, fingerprint
				))))
			})?;

			let name = shape_field::SHAPE.get_utf8(&field_entry.row, shape_field::NAME).to_string();
			let base_type = shape_field::SHAPE.get_u8(&field_entry.row, shape_field::TYPE);
			let constraint_type = shape_field::SHAPE.get_u8(&field_entry.row, shape_field::CONSTRAINT_TYPE);
			let constraint_param1 =
				shape_field::SHAPE.get_u32(&field_entry.row, shape_field::CONSTRAINT_P1);
			let constraint_param2 =
				shape_field::SHAPE.get_u32(&field_entry.row, shape_field::CONSTRAINT_P2);
			let constraint = TypeConstraint::from_ffi(FFITypeConstraint {
				base_type,
				constraint_type,
				constraint_param1,
				constraint_param2,
			});
			let offset = shape_field::SHAPE.get_u32(&field_entry.row, shape_field::OFFSET);
			let size = shape_field::SHAPE.get_u32(&field_entry.row, shape_field::SIZE);
			let align = shape_field::SHAPE.get_u8(&field_entry.row, shape_field::ALIGN);

			fields.push(RowShapeField {
				name,
				constraint,
				offset,
				size,
				align,
			});
		}

		let shape = RowShape::from_parts(fingerprint, fields);
		Span::current().record("cache_hit", false);
		Span::current().record("field_count", shape.field_count());
		self.cache.set_row_shape(shape.clone());

		Ok(Some(shape))
	}

	pub fn find_row_shape(&self, fingerprint: RowShapeFingerprint) -> Option<RowShape> {
		self.cache.find_row_shape(fingerprint)
	}

	pub fn get_or_create_row_shape_pending(
		&self,
		pending: &mut Vec<RowShape>,
		fields: Vec<RowShapeField>,
	) -> RowShape {
		let shape = RowShape::new(fields);
		let fingerprint = shape.fingerprint();

		if let Some(cached) = self.cache.find_row_shape(fingerprint) {
			return cached;
		}

		self.cache.set_row_shape(shape.clone());
		pending.push(shape.clone());

		shape
	}

	pub fn persist_pending_shapes(&self, txn: &mut Transaction<'_>, shapes: Vec<RowShape>) -> Result<()> {
		for shape in shapes {
			let fingerprint = shape.fingerprint();

			if find_row_shape_by_fingerprint(txn, fingerprint)?.is_some() {
				continue;
			}

			create_row_shape(txn, &shape)?;
		}
		Ok(())
	}
}
