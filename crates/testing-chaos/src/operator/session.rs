// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::interface::change::Change;
use reifydb_value::{Result, value::Value};

use crate::operator::{subject::Subject, view::View};

/// One operator under test plus the view its emissions fold into.
///
/// Both chaos families drive an operator by pushing changes at it and reading what comes back, and
/// both need the emissions folded into a view that flags a diff stream it cannot make sense of. Only
/// the way they GENERATE the changes differs - a window family rolls a step mix over coordinates, a
/// guest family replays an event log carrying its own mutation primitives - and that difference is
/// real, because the two model different upstream behaviour. So the generation stays with each family
/// and the execution lives here.
pub struct Session<'a, S: Subject> {
	subject: &'a mut S,
	view: View,
}

impl<'a, S: Subject> Session<'a, S> {
	pub fn new(subject: &'a mut S) -> Self {
		Self {
			subject,
			view: View::new(),
		}
	}

	/// Applies a change and folds whatever the operator emitted into the view.
	pub fn apply(&mut self, change: Change) -> Result<()> {
		let out = self.subject.apply(change)?;
		self.view.apply(&out);
		Ok(())
	}

	/// Advances to `at_ms` and folds any emission. Returns whether anything was emitted, which is what
	/// a drain loop needs to decide it has reached quiescence.
	pub fn tick(&mut self, at_ms: u64) -> Result<bool> {
		match self.subject.tick(at_ms)? {
			Some(change) => {
				self.view.apply(&change);
				Ok(true)
			}
			None => Ok(false),
		}
	}

	/// Ticks at `at_ms` until the view stops changing.
	///
	/// Stops on a view that stopped moving rather than on an empty emission: an operator may emit a
	/// change whose net effect on the view is nothing, and treating that as progress would spin.
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

	pub fn into_view(self) -> View {
		self.view
	}
}
