// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::internal_err;
use reifydb_value::{Result, value::row_number::RowNumber};

#[derive(Debug, Clone, PartialEq)]
pub struct WindowColumns<G, C, X> {
	index: Vec<usize>,
	rows: Vec<Option<RowNumber>>,
	groups: Vec<G>,
	coords: Vec<Option<C>>,
	contributions: Vec<X>,
}

impl<G, C, X> WindowColumns<G, C, X> {
	pub fn new(
		index: Vec<usize>,
		rows: Vec<Option<RowNumber>>,
		groups: Vec<G>,
		coords: Vec<Option<C>>,
		contributions: Vec<X>,
	) -> Result<Self> {
		let len = index.len();
		if rows.len() != len || groups.len() != len || coords.len() != len || contributions.len() != len {
			return internal_err!(
				"window columns must share one length (index={len}, rows={}, groups={}, coords={}, \
				 contributions={})",
				rows.len(),
				groups.len(),
				coords.len(),
				contributions.len()
			);
		}
		Ok(Self {
			index,
			rows,
			groups,
			coords,
			contributions,
		})
	}

	pub fn len(&self) -> usize {
		self.index.len()
	}

	pub fn is_empty(&self) -> bool {
		self.index.is_empty()
	}

	pub fn index(&self) -> &[usize] {
		&self.index
	}

	pub fn rows(&self) -> &[Option<RowNumber>] {
		&self.rows
	}

	pub fn groups(&self) -> &[G] {
		&self.groups
	}

	pub fn coords(&self) -> &[Option<C>] {
		&self.coords
	}

	pub fn contributions(&self) -> &[X] {
		&self.contributions
	}
}

#[derive(Debug, Clone, PartialEq)]
pub struct RetractRows<G> {
	rows: Vec<Option<RowNumber>>,
	groups: Vec<G>,
}

impl<G> RetractRows<G> {
	pub fn new(rows: Vec<Option<RowNumber>>, groups: Vec<G>) -> Result<Self> {
		if rows.len() != groups.len() {
			return internal_err!(
				"retract rows must share one length (rows={}, groups={})",
				rows.len(),
				groups.len()
			);
		}
		Ok(Self {
			rows,
			groups,
		})
	}

	pub fn len(&self) -> usize {
		self.rows.len()
	}

	pub fn is_empty(&self) -> bool {
		self.rows.is_empty()
	}

	pub fn rows(&self) -> &[Option<RowNumber>] {
		&self.rows
	}

	pub fn groups(&self) -> &[G] {
		&self.groups
	}
}

#[derive(Debug, Clone, PartialEq)]
pub enum WindowDiff<G, C, X> {
	Insert(WindowColumns<G, C, X>),
	Update {
		pre: WindowColumns<G, C, X>,
		post: WindowColumns<G, C, X>,
	},
	Remove(WindowColumns<G, C, X>),
	Retract(RetractRows<G>),
}

pub trait WindowRows {
	type Group;
	type Coord;
	type Contribution;

	fn diff_count(&self) -> usize;

	fn diff(&mut self, index: usize) -> Result<WindowDiff<Self::Group, Self::Coord, Self::Contribution>>;
}
