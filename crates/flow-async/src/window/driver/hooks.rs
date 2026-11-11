// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::key::encoded::EncodedKey;
use reifydb_value::{
	Result,
	util::hash::Hash128,
	value::{datetime::DateTime, row_number::RowNumber},
};

use crate::window::{accumulator::WindowAccumulator, span::WindowSpan};

pub struct WindowEmits<Row> {
	pub inserts: Vec<(RowNumber, DateTime, Row)>,
	pub updates: Vec<(RowNumber, DateTime, Row, Row)>,
	pub removes: Vec<(RowNumber, DateTime, Row)>,
}

impl<Row> Default for WindowEmits<Row> {
	fn default() -> Self {
		Self {
			inserts: Vec::new(),
			updates: Vec::new(),
			removes: Vec::new(),
		}
	}
}

pub trait WindowHooks {
	type Group;
	type Coord;
	type Accumulator: WindowAccumulator;
	type Row;
	type Out;

	fn new_accumulator(&self) -> Self::Accumulator;

	fn group_identity(&self, group: &Self::Group) -> (Hash128, EncodedKey);

	fn build(
		&self,
		group: &Self::Group,
		span: WindowSpan<Self::Coord>,
		value: &<Self::Accumulator as WindowAccumulator>::Output,
	) -> Option<Self::Row>;

	fn emit(&mut self, emits: WindowEmits<Self::Row>) -> Result<Self::Out>;
}
