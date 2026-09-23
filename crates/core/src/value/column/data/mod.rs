// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub mod canonical;

use std::{any::Any, sync::Arc};

use arrow_buffer::NullBuffer;
use canonical::Canonical;
use reifydb_value::{Result, value::Value};

use crate::value::column::encoding::EncodingId;

pub trait ColumnData: Send + Sync + 'static {
	fn len(&self) -> usize;
	fn encoding(&self) -> EncodingId;

	fn nones(&self) -> Option<&NullBuffer>;

	fn get_value(&self, idx: usize) -> Value;
	fn as_string(&self, idx: usize) -> String;

	fn as_any(&self) -> &dyn Any;

	fn to_canonical(&self) -> Result<Arc<Canonical>>;

	fn slice(&self, start: usize, end: usize) -> Result<Column> {
		let canon = self.to_canonical()?;
		Ok(Column::from_canonical(canonical_slice(&canon, start, end)?))
	}
}

#[derive(Clone)]
pub struct Column(Arc<dyn ColumnData>);

impl Column {
	pub fn from_data(data: Arc<dyn ColumnData>) -> Self {
		Self(data)
	}

	pub fn from_canonical(canon: Canonical) -> Self {
		Self(Arc::new(canon))
	}

	pub fn data(&self) -> &dyn ColumnData {
		&*self.0
	}

	pub fn len(&self) -> usize {
		self.0.len()
	}

	pub fn encoding(&self) -> EncodingId {
		self.0.encoding()
	}

	pub fn nones(&self) -> Option<&NullBuffer> {
		self.0.nones()
	}

	pub fn to_canonical(&self) -> Result<Arc<Canonical>> {
		self.0.to_canonical()
	}

	pub fn slice(&self, start: usize, end: usize) -> Result<Column> {
		self.0.slice(start, end)
	}
}

fn canonical_slice(canon: &Canonical, start: usize, end: usize) -> Result<Canonical> {
	assert!(start <= end);
	assert!(end <= canon.len());
	let new_buffer = canon.buffer.slice(start, end);
	Ok(Canonical::new(canon.ty.clone(), canon.nullable, new_buffer))
}
