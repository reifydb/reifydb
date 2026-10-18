// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{iter::once, sync::Arc};

use arrow_array::ArrayRef;
use arrow_schema::FieldRef;
use reifydb_catalog::store::series::shape::deserialize_encoding_config;
use reifydb_core::{
	interface::catalog::series::{Series, SeriesEncoding},
	value::batch::batch,
};
use reifydb_extension::block_encoding::{error::BlockEncodingError, registry::BlockEncodings};
use reifydb_store_column::{
	block::{BlockData, BlockEncodingHeader, BlockUserColumn},
	compress::Compressor,
};
use reifydb_value::{
	fragment::Fragment,
	reifydb_assertions,
	value::{
		system_columns::SystemColumn,
		value_type::{ValueType, field::from_field},
	},
};

use crate::Result;

pub fn compress_columns(
	columns: Vec<(String, ValueType, (FieldRef, ArrayRef))>,
	compressor: &Compressor,
) -> Result<BlockData> {
	let mut schema = Vec::with_capacity(columns.len());
	let mut chunked = Vec::with_capacity(columns.len());
	for (name, ty, column) in columns {
		let chunks = compressor.compress(ty.clone(), &column)?;
		schema.push((name, ty, chunks.nullable));
		chunked.push(chunks);
	}
	Ok(BlockData::new(Arc::new(schema), chunked))
}

pub fn encode_series_block(
	columns: Vec<(String, ValueType, (FieldRef, ArrayRef))>,
	series: &Series,
	encoding: &SeriesEncoding,
	encodings: &BlockEncodings,
	compressor: &Compressor,
) -> Result<BlockData> {
	let (system, mut user): (Vec<_>, Vec<_>) =
		columns.into_iter().partition(|(name, _, _)| SystemColumn::from_name(name).is_some());
	let name = encoding.name.as_str();
	let expected = series.encoding_user_fields();
	if user.iter().all(|(_, _, (_, array))| array.is_empty()) {
		for ((column, _, (field, _)), want) in user.iter_mut().zip(&expected) {
			if column == want.name() {
				*field = Arc::clone(want);
			}
		}
	}
	let user_batch = batch(user.iter().map(|(_, _, column)| column.clone()).collect())?;
	if user_batch.schema().fields().iter().ne(expected.iter()) {
		return Err(BlockEncodingError::UserFieldsDiffer {
			name: name.to_string(),
			series: series.name.clone(),
		}
		.into());
	}
	let block_encoding = encodings.get(name).ok_or_else(|| BlockEncodingError::UnknownName {
		name: name.to_string(),
		fragment: Fragment::None,
	})?;
	let config = deserialize_encoding_config(&encoding.name, &encoding.config)?;
	let bound = block_encoding.bind(user_batch.schema().fields(), &config)?;
	let stored = bound.encode(user_batch.clone())?;
	if stored.num_rows() != user_batch.num_rows() {
		return Err(BlockEncodingError::RowCountChanged {
			name: name.to_string(),
			before: user_batch.num_rows(),
			after: stored.num_rows(),
		}
		.into());
	}
	if stored.schema().fields().iter().ne(bound.stored_fields().iter()) {
		return Err(BlockEncodingError::StoredFieldsDiffer {
			name: name.to_string(),
		}
		.into());
	}
	for column in once(series.key.column()).chain(series.tag.is_some().then_some("tag")) {
		let changed = || BlockEncodingError::PassThroughValuesChanged {
			name: name.to_string(),
			column: column.to_string(),
		};
		let user_column = user_batch.column_by_name(column).ok_or_else(changed)?;
		match stored.column_by_name(column) {
			Some(stored_column) if stored_column.as_ref() == user_column.as_ref() => {}
			_ => return Err(changed().into()),
		}
	}
	reifydb_assertions! {
		let decoded = bound.decode(block_encoding.version(), stored.clone())?;
		assert!(
			decoded == user_batch,
			"block encoding '{name}' is lossy: decode(encode(block)) differs from the sealed block, so a column read would return rows that were never written"
		);
	}
	let mut user_columns = Vec::with_capacity(user.len());
	for (column, ty, (field, _)) in &user {
		user_columns.push(BlockUserColumn {
			name: column.clone(),
			ty: ty.clone(),
			nullable: field.is_nullable(),
			field_type: from_field(field)?,
		});
	}
	let mut out = Vec::with_capacity(stored.num_columns() + system.len());
	for (field, array) in stored.schema().fields().iter().zip(stored.columns()) {
		let ty = from_field(field)?.value_type.ok_or_else(|| BlockEncodingError::UnsupportedStoredType {
			name: name.to_string(),
			field: field.name().clone(),
		})?;
		out.push((field.name().clone(), ty, (field.clone(), array.clone())));
	}
	out.extend(system);
	let header = BlockEncodingHeader {
		name: name.to_string(),
		version: block_encoding.version(),
		config: encoding.config.clone(),
		user: user_columns,
	};
	Ok(compress_columns(out, compressor)?.with_block_encoding(Some(header)))
}
