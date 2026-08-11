// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::marker::PhantomData;

use reifydb_value::value::row_number::RowNumber;

use crate::{
	error::SdkError,
	flow::operator::{
		column::row::Row,
		context::{OperatorContext, RowEmit, UpdateEmit},
	},
};

pub struct InsertBatch<'a, R: Row, O: OperatorContext + 'a> {
	emit: O::InsertEmit<'a>,
	row_numbers: Vec<RowNumber>,
	_row: PhantomData<R>,
}

impl<'a, R: Row, O: OperatorContext + 'a> InsertBatch<'a, R, O> {
	pub fn new(ctx: &'a mut O, row_capacity: usize) -> Result<Self, SdkError> {
		Ok(Self {
			emit: ctx.insert_emit::<R>(row_capacity)?,
			row_numbers: Vec::with_capacity(row_capacity),
			_row: PhantomData,
		})
	}

	pub fn push(&mut self, row_number: RowNumber, row: &R) -> Result<(), SdkError> {
		row.encode_into(self.emit.sink())?;
		self.row_numbers.push(row_number);
		Ok(())
	}

	#[inline]
	pub fn len(&self) -> usize {
		self.row_numbers.len()
	}

	#[inline]
	pub fn is_empty(&self) -> bool {
		self.row_numbers.is_empty()
	}

	pub fn finish(self) -> Result<(), SdkError> {
		if self.row_numbers.is_empty() {
			return Ok(());
		}
		self.emit.finish(&self.row_numbers)
	}
}

pub struct UpdateBatch<'a, R: Row, O: OperatorContext + 'a> {
	emit: O::UpdateEmit<'a>,
	row_numbers: Vec<RowNumber>,
	_row: PhantomData<R>,
}

impl<'a, R: Row, O: OperatorContext + 'a> UpdateBatch<'a, R, O> {
	pub fn new(ctx: &'a mut O, row_capacity: usize) -> Result<Self, SdkError> {
		Ok(Self {
			emit: ctx.update_emit::<R>(row_capacity)?,
			row_numbers: Vec::with_capacity(row_capacity),
			_row: PhantomData,
		})
	}

	pub fn push(&mut self, row_number: RowNumber, pre_row: &R, post_row: &R) -> Result<(), SdkError> {
		pre_row.encode_into(self.emit.pre())?;
		post_row.encode_into(self.emit.post())?;
		self.row_numbers.push(row_number);
		Ok(())
	}

	#[inline]
	pub fn len(&self) -> usize {
		self.row_numbers.len()
	}

	#[inline]
	pub fn is_empty(&self) -> bool {
		self.row_numbers.is_empty()
	}

	pub fn finish(self) -> Result<(), SdkError> {
		if self.row_numbers.is_empty() {
			return Ok(());
		}
		self.emit.finish(&self.row_numbers)
	}
}

pub struct RemoveBatch<'a, R: Row, O: OperatorContext + 'a> {
	emit: O::RemoveEmit<'a>,
	row_numbers: Vec<RowNumber>,
	_row: PhantomData<R>,
}

impl<'a, R: Row, O: OperatorContext + 'a> RemoveBatch<'a, R, O> {
	pub fn new(ctx: &'a mut O, row_capacity: usize) -> Result<Self, SdkError> {
		Ok(Self {
			emit: ctx.remove_emit::<R>(row_capacity)?,
			row_numbers: Vec::with_capacity(row_capacity),
			_row: PhantomData,
		})
	}

	pub fn push(&mut self, row_number: RowNumber, row: &R) -> Result<(), SdkError> {
		row.encode_into(self.emit.sink())?;
		self.row_numbers.push(row_number);
		Ok(())
	}

	#[inline]
	pub fn len(&self) -> usize {
		self.row_numbers.len()
	}

	#[inline]
	pub fn is_empty(&self) -> bool {
		self.row_numbers.is_empty()
	}

	pub fn finish(self) -> Result<(), SdkError> {
		if self.row_numbers.is_empty() {
			return Ok(());
		}
		self.emit.finish(&self.row_numbers)
	}
}
