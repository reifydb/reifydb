// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{HashMap, HashSet, hash_map::Entry},
	hash::Hash,
};

use reifydb_core::internal_err;
use reifydb_value::{Result, value::row_number::RowNumber};

use crate::{
	operator::state::seal::coord::Coord,
	window::{
		accumulator::{AccumulatorEvents, AccumulatorOp, Contributions},
		driver::rows::{WindowColumns, WindowDiff, WindowRows},
		engine::tumbling::TumblingBuckets,
		span::WindowSpan,
	},
};

pub struct Routed<G, C, X> {
	pub buckets: TumblingBuckets<G, C, X>,
	pub arrival: Vec<(G, WindowSpan<C>)>,
	pub window_max_coord: HashMap<(G, WindowSpan<C>), C>,
}

struct Routing<G, C: Coord, X> {
	size: C::Span,
	index: HashMap<(G, WindowSpan<C>), usize>,
	windows: Vec<((G, WindowSpan<C>), AccumulatorEvents<X>, Option<C>)>,
}

impl<G, C, X> Routing<G, C, X>
where
	G: Clone + Ord + Hash,
	C: Coord + Hash,
	X: Contributions,
{
	fn push(&mut self, op: AccumulatorOp, group: &G, coord: Option<C>, contribution: &X::Item) {
		let Some(coord) = coord else {
			return;
		};
		let key = (group.clone(), WindowSpan::for_coord(coord, self.size));
		let at = match self.index.entry(key.clone()) {
			Entry::Occupied(slot) => *slot.get(),
			Entry::Vacant(slot) => {
				self.windows.push((key, AccumulatorEvents::new(), None));
				*slot.insert(self.windows.len() - 1)
			}
		};
		let (_, events, latest) = &mut self.windows[at];
		if op == AccumulatorOp::Add {
			*latest = Some(latest.map_or(coord, |latest| latest.max(coord)));
		}
		events.push(op, contribution.clone());
	}

	fn push_row(&mut self, op: AccumulatorOp, columns: &WindowColumns<G, C, X::Item>, row: usize) {
		self.push(op, &columns.groups()[row], columns.coords()[row], &columns.contributions()[row]);
	}

	fn push_all(&mut self, op: AccumulatorOp, columns: &WindowColumns<G, C, X::Item>) {
		for row in 0..columns.len() {
			self.push_row(op, columns, row);
		}
	}

	fn push_update(
		&mut self,
		diff: usize,
		pre: &WindowColumns<G, C, X::Item>,
		post: &WindowColumns<G, C, X::Item>,
	) -> Result<()> {
		let number = |columns: &WindowColumns<G, C, X::Item>, row: usize| -> Result<RowNumber> {
			match columns.rows()[row] {
				Some(number) => Ok(number),
				None => internal_err!(
					"update diff {diff} has a row without a row number at batch position {}",
					columns.index()[row]
				),
			}
		};
		let mut seen: HashSet<RowNumber> = HashSet::new();
		let mut run_pre: Vec<usize> = Vec::new();
		let mut run_post: Vec<usize> = Vec::new();
		let (mut p, mut q) = (0, 0);
		while p < pre.len() || q < post.len() {
			let position = match (pre.index().get(p), post.index().get(q)) {
				(Some(&a), Some(&b)) => a.min(b),
				(Some(&a), None) => a,
				(None, Some(&b)) => b,
				(None, None) => break,
			};
			let before = (pre.index().get(p) == Some(&position)).then_some(p);
			let after = (post.index().get(q) == Some(&position)).then_some(q);
			let mut numbers = Vec::with_capacity(2);
			if let Some(row) = before {
				numbers.push(number(pre, row)?);
			}
			if let Some(row) = after {
				numbers.push(number(post, row)?);
			}
			if numbers.iter().any(|n| seen.contains(n)) {
				self.flush_run(pre, post, &mut run_pre, &mut run_post);
				seen.clear();
			}
			seen.extend(numbers);
			if let Some(row) = before {
				run_pre.push(row);
				p += 1;
			}
			if let Some(row) = after {
				run_post.push(row);
				q += 1;
			}
		}
		self.flush_run(pre, post, &mut run_pre, &mut run_post);
		Ok(())
	}

	fn flush_run(
		&mut self,
		pre: &WindowColumns<G, C, X::Item>,
		post: &WindowColumns<G, C, X::Item>,
		run_pre: &mut Vec<usize>,
		run_post: &mut Vec<usize>,
	) {
		for row in run_pre.drain(..) {
			self.push_row(AccumulatorOp::Remove, pre, row);
		}
		for row in run_post.drain(..) {
			self.push_row(AccumulatorOp::Add, post, row);
		}
	}

	fn finish(self) -> Routed<G, C, X> {
		let mut routed = Routed {
			buckets: TumblingBuckets::new(),
			arrival: Vec::with_capacity(self.windows.len()),
			window_max_coord: HashMap::with_capacity(self.windows.len()),
		};
		for (key, events, latest) in self.windows {
			routed.arrival.push(key.clone());
			if let Some(latest) = latest {
				routed.window_max_coord.insert(key.clone(), latest);
			}
			routed.buckets.insert(key, events);
		}
		routed
	}
}

pub fn route_tumbling<R, X>(rows: &mut R, size: <R::Coord as Coord>::Span) -> Result<Routed<R::Group, R::Coord, X>>
where
	R: WindowRows,
	X: Contributions<Item = R::Contribution>,
	R::Group: Clone + Ord + Hash,
	R::Coord: Coord + Hash,
{
	let mut routing = Routing {
		size,
		index: HashMap::new(),
		windows: Vec::new(),
	};
	for diff in 0..rows.diff_count() {
		match rows.diff(diff)? {
			WindowDiff::Insert(post) => routing.push_all(AccumulatorOp::Add, &post),
			WindowDiff::Remove(pre) => routing.push_all(AccumulatorOp::Remove, &pre),
			WindowDiff::Update {
				pre,
				post,
			} => routing.push_update(diff, &pre, &post)?,
		}
	}
	Ok(routing.finish())
}
