// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::interface::change::Change;
use reifydb_value::{Result, value::Value};

use crate::operator::{
	reclaim::{Reclaimed, StateFootprint},
	subject::Subject,
	view::MaterializedView,
};

pub struct Session<'a, S: Subject> {
	subject: &'a mut S,
	view: MaterializedView,
}

impl<'a, S: Subject> Session<'a, S> {
	pub fn new(subject: &'a mut S) -> Self {
		Self {
			subject,
			view: MaterializedView::empty(),
		}
	}

	pub fn apply(&mut self, change: Change) -> Result<()> {
		let out = self.subject.apply(change)?;
		self.view.fold(&out);
		Ok(())
	}

	pub fn reclaim(&mut self, at_ms: u64) -> Result<Reclaimed> {
		self.subject.reclaim(at_ms)
	}

	pub fn footprint(&mut self) -> Result<Option<StateFootprint>> {
		self.subject.footprint()
	}

	pub fn tick(&mut self, at_ms: u64) -> Result<bool> {
		match self.subject.tick(at_ms)? {
			Some(change) => {
				self.view.fold(&change);
				Ok(true)
			}
			None => Ok(false),
		}
	}

	pub fn drain(&mut self, at_ms: u64, max_ticks: usize) -> Result<usize> {
		let mut ticks = 0;
		loop {
			let before = self.view.len();
			self.tick(at_ms)?;
			ticks += 1;
			if self.view.len() == before || self.view.is_empty() {
				return Ok(ticks);
			}
			assert!(ticks < max_ticks, "expiry did not reach quiescence within {ticks} ticks");
		}
	}

	pub fn view(&self) -> &MaterializedView {
		&self.view
	}

	pub fn projected(&self, indices: &[usize]) -> Vec<Vec<Value>> {
		self.view.projected(indices)
	}

	pub fn incoherent(&self) -> &[String] {
		&self.view.incoherent
	}

	pub fn len(&self) -> usize {
		self.view.len()
	}

	pub fn is_empty(&self) -> bool {
		self.view.is_empty()
	}

	pub fn into_view(self) -> MaterializedView {
		self.view
	}
}
