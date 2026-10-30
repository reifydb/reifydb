// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::{
	constraint::decode_type_constraint_info,
	reader::Reader,
	row::{
		pod::EncodedPodRow,
		shape::{RowFamily, RowShape, RowShapeField, fingerprint::RowShapeFingerprint},
	},
};
use reifydb_core::{
	error::diagnostic::internal::internal,
	interface::catalog::column::Column,
	key::row::{RowShapeFieldKey, RowShapeKey},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{error::Error, value::constraint::TypeConstraint};
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

		let header_key = RowShapeKey::new(fingerprint);
		let header_entry = match txn.get(&[header_key.clone().into()])?.pop().flatten() {
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
			let field_key = RowShapeFieldKey::new(fingerprint, i as u16);
			let field_entry = txn.get(&[field_key.clone().into()])?.pop().flatten().ok_or_else(|| {
				Error(Box::new(internal(format!(
					"RowShape field {} missing for fingerprint {:?}",
					i, fingerprint
				))))
			})?;

			let name = shape_field::SHAPE.get_utf8(&field_entry.bytes, shape_field::NAME).to_string();
			let constraint = decode_type_constraint_info(&mut Reader::new(
				shape_field::SHAPE.get_blob_slice(&field_entry.bytes, shape_field::TYPE_CONSTRAINT),
			))
			.unwrap_or_else(|error| panic!("invalid persisted type of row shape field {name}: {error}"));
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

	pub(crate) fn column_shape_fields<'a>(
		&self,
		txn: &mut Transaction<'_>,
		columns: impl IntoIterator<Item = &'a Column>,
	) -> Result<Vec<RowShapeField>> {
		let mut fields = Vec::new();
		for col in columns {
			let constraint = match col.dictionary_id {
				Some(dict_id) => match self.find_dictionary(txn, dict_id)? {
					Some(dict) => TypeConstraint::dictionary(dict_id, dict.id_type),
					None => col.constraint.clone(),
				},
				None => col.constraint.clone(),
			};
			fields.push(RowShapeField::new(col.name.clone(), constraint));
		}
		Ok(fields)
	}
}
