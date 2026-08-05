// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub mod row_shape;

use reifydb_codec::{
	encoded::shape::{RowShape, fingerprint::RowShapeFingerprint},
	tag::value_type_from_tag_byte,
};
use reifydb_value::value::constraint::{
	Constraint, TypeConstraint, bytes::MaxBytes, precision::Precision, scale::Scale,
};

use crate::{error::SdkError, operator::context::ffi::FFIOperatorContext};

pub struct RowShapeResolver<'a> {
	ctx: &'a mut FFIOperatorContext,
}

impl<'a> RowShapeResolver<'a> {
	pub(crate) fn new(ctx: &'a mut FFIOperatorContext) -> Self {
		Self {
			ctx,
		}
	}

	pub fn find_row_shape(&self, fingerprint: RowShapeFingerprint) -> Result<Option<RowShape>, SdkError> {
		row_shape::raw_catalog_find_row_shape(self.ctx, fingerprint)
	}
}

pub(crate) fn decode_type_constraint(
	base_type: u8,
	constraint_type: u8,
	param1: u32,
	param2: u32,
) -> Result<TypeConstraint, SdkError> {
	let ty = value_type_from_tag_byte(base_type);

	match constraint_type {
		0 => Ok(TypeConstraint::unconstrained(ty)),
		1 => Ok(TypeConstraint::with_constraint(ty, Constraint::MaxBytes(MaxBytes::new(param1)))),
		2 => Ok(TypeConstraint::with_constraint(
			ty,
			Constraint::PrecisionScale(Precision::new(param1 as u8), Scale::new(param2 as u8)),
		)),
		_ => Err(SdkError::Other("Invalid constraint type".to_string())),
	}
}
