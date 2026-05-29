// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

pub mod namespace;
pub mod row_shape;
pub mod table;

use std::{slice::from_raw_parts, str};

use reifydb_abi::catalog::{column::ColumnFFI, primary_key::PrimaryKeyFFI};
use reifydb_core::{
	common::CommitVersion,
	encoded::shape::{RowShape, fingerprint::RowShapeFingerprint},
	interface::catalog::{
		column::{Column, ColumnIndex},
		id::{ColumnId, NamespaceId, PrimaryKeyId, TableId},
		key::PrimaryKey,
		namespace::Namespace,
		table::Table,
	},
};
use reifydb_value::value::{
	constraint::{Constraint, TypeConstraint, bytes::MaxBytes, precision::Precision, scale::Scale},
	value_type::ValueType,
};

use crate::{error::SdkError, operator::context::ffi::FFIOperatorContext};

pub struct Catalog<'a> {
	ctx: &'a mut FFIOperatorContext,
}

impl<'a> Catalog<'a> {
	pub(crate) fn new(ctx: &'a mut FFIOperatorContext) -> Self {
		Self {
			ctx,
		}
	}

	pub fn find_namespace(
		&self,
		namespace: NamespaceId,
		version: CommitVersion,
	) -> Result<Option<Namespace>, SdkError> {
		namespace::raw_catalog_find_namespace(self.ctx, namespace, version)
	}

	pub fn find_namespace_by_name(
		&self,
		namespace: &str,
		version: CommitVersion,
	) -> Result<Option<Namespace>, SdkError> {
		namespace::raw_catalog_find_namespace_by_name(self.ctx, namespace, version)
	}

	pub fn find_table(&self, table: TableId, version: CommitVersion) -> Result<Option<Table>, SdkError> {
		table::raw_catalog_find_table(self.ctx, table, version)
	}

	pub fn find_table_by_name(
		&self,
		namespace: NamespaceId,
		name: &str,
		version: CommitVersion,
	) -> Result<Option<Table>, SdkError> {
		table::raw_catalog_find_table_by_name(self.ctx, namespace, name, version)
	}

	pub fn find_row_shape(&self, fingerprint: RowShapeFingerprint) -> Result<Option<RowShape>, SdkError> {
		row_shape::raw_catalog_find_row_shape(self.ctx, fingerprint)
	}
}

pub(crate) unsafe fn unmarshal_column(ffi_col: &ColumnFFI) -> Result<Column, SdkError> {
	let name_bytes = if !ffi_col.name.ptr.is_null() && ffi_col.name.len > 0 {
		unsafe { from_raw_parts(ffi_col.name.ptr, ffi_col.name.len) }
	} else {
		&[]
	};

	let name = str::from_utf8(name_bytes)
		.map_err(|_| SdkError::Other("Invalid UTF-8 in column name".to_string()))?
		.to_string();

	let constraint = decode_type_constraint(
		ffi_col.base_type,
		ffi_col.constraint_type,
		ffi_col.constraint_param1,
		ffi_col.constraint_param2,
	)?;

	Ok(Column {
		id: ColumnId(ffi_col.id),
		name,
		constraint,
		properties: Vec::new(),
		index: ColumnIndex(ffi_col.column_index),
		auto_increment: ffi_col.auto_increment != 0,
		dictionary_id: None,
	})
}

pub(crate) unsafe fn unmarshal_primary_key(ffi_pk: &PrimaryKeyFFI) -> Result<PrimaryKey, SdkError> {
	let column_ids = if !ffi_pk.column_ids.is_null() && ffi_pk.column_count > 0 {
		unsafe { from_raw_parts(ffi_pk.column_ids, ffi_pk.column_count).to_vec() }
	} else {
		Vec::new()
	};

	let columns = column_ids
		.into_iter()
		.enumerate()
		.map(|(idx, col_id)| Column {
			id: ColumnId(col_id),
			name: format!("col_{}", col_id),
			constraint: TypeConstraint::unconstrained(ValueType::Int4),
			properties: Vec::new(),
			index: ColumnIndex(idx as u8),
			auto_increment: false,
			dictionary_id: None,
		})
		.collect();

	Ok(PrimaryKey {
		id: PrimaryKeyId(ffi_pk.id),
		columns,
	})
}

pub(crate) fn decode_type_constraint(
	base_type: u8,
	constraint_type: u8,
	param1: u32,
	param2: u32,
) -> Result<TypeConstraint, SdkError> {
	let ty = ValueType::from_u8(base_type);

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
