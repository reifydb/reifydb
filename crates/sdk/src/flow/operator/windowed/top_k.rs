// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::BTreeMap, fmt::Debug, hash::Hash};

use reifydb_codec::{
	key::encoded::{EncodedKey, IntoEncodedKey},
	row::operator::state::StateCodec,
};
use reifydb_core::{
	error::CoreError,
	interface::{catalog::flow::OperatorId, flow::OperatorCapability},
	metrics::heap::{HeapSize, OperatorSample},
	operator_with::ApplyWith,
};
use reifydb_flow::{
	operator::state::seal::{coord::Coord, domain::SealDomain, rule::is_sealed},
	window::{
		accumulator::WindowAccumulator,
		engine::{
			AccumulatorEvent,
			rolling::{RollingBuckets, RollingBuffer},
			rolling_top_k::{RollingTopKEngine, TopKEmit},
		},
		settings::WindowSettings,
		span::WindowSpan,
	},
};
use reifydb_value::{
	config::ExtensionParams,
	error::Error,
	value::{diff_type::DiffType, row_number::RowNumber},
};
use tracing::debug;

use crate::{
	error::Result,
	flow::operator::{
		MountedOperator, OperatorMetadata,
		column::{
			batch::{InsertBatch, RemoveBatch, UpdateBatch},
			operator::OperatorColumn,
			row::OutputRows,
		},
		context::{GuestContext, Windowed},
		timer::Timer,
		view::{ChangeView, ColumnsView, DiffView},
		windowed::{
			bucket_of,
			guest_as_host::GuestAsHost,
			observe_batch,
			operator::{Contribution, Emit, KindSet, WindowedOperator},
			seal_frontier, timer_frontier, window_engine_config,
		},
	},
};

type SealSpan<A> = <<A as WindowedOperator>::Coord as SealDomain>::SealSpan;
type Sk<A> = <<A as WindowedOperator>::Output as OutputRows>::Key;
type Rw<A> = <<A as WindowedOperator>::Output as OutputRows>::Row;
type Buckets<A> = RollingBuckets<<A as WindowedOperator>::GroupKey, <A as WindowedOperator>::Coord, Contribution<A>>;
type TopKEngine<A> = RollingTopKEngine<
	<A as WindowedOperator>::GroupKey,
	<A as WindowedOperator>::Coord,
	<A as WindowedOperator>::Accumulator,
	Sk<A>,
	Rw<A>,
>;

pub struct TopKDriver<A>
where
	A: Emit,
	A::Output: OutputRows + IntoIterator<Item = (Sk<A>, Rw<A>)>,
	for<'a> &'a A::GroupKey: IntoEncodedKey,
	for<'a> &'a Sk<A>: IntoEncodedKey,
{
	aggregator: A,
	engine: TopKEngine<A>,
	pane: <A::Coord as Coord>::Span,
	seal_span: Option<SealSpan<A>>,
	settings: WindowSettings<A::Coord>,
}

impl<A> TopKDriver<A>
where
	A: Emit,
	A::Output: OutputRows + IntoIterator<Item = (Sk<A>, Rw<A>)>,
	for<'a> &'a A::GroupKey: IntoEncodedKey,
	for<'a> &'a Sk<A>: IntoEncodedKey,
{
	fn row_key(group: &A::GroupKey, secondary: &Sk<A>) -> EncodedKey {
		EncodedKey::builder()
			.raw(group.into_encoded_key().as_slice())
			.raw(secondary.into_encoded_key().as_slice())
			.build()
	}

	fn route(&self, ctx: &mut impl GuestContext, change: &impl ChangeView) -> Buckets<A> {
		let mut buckets: Buckets<A> = BTreeMap::new();

		for di in 0..change.diff_count() {
			let Some(diff) = change.diff(di) else {
				continue;
			};
			match diff.kind() {
				DiffType::Insert => {
					if let Some(cols) = diff.post() {
						self.push_all(ctx, &cols, &mut buckets, true);
					}
				}
				DiffType::Update => {
					if let (Some(pre), Some(post)) = (diff.pre(), diff.post()) {
						self.push_all(ctx, &pre, &mut buckets, false);
						self.push_all(ctx, &post, &mut buckets, true);
					}
				}
				DiffType::Remove => {
					if let Some(cols) = diff.pre() {
						self.push_all(ctx, &cols, &mut buckets, false);
					}
				}
			}
		}
		buckets
	}

	fn push_all<C: ColumnsView>(
		&self,
		ctx: &mut impl GuestContext,
		cols: &C,
		buckets: &mut Buckets<A>,
		is_add: bool,
	) {
		for i in 0..cols.row_count() {
			let Some(row) = cols.row(i) else {
				continue;
			};
			let Some(coord) = self.aggregator.coord(&row) else {
				continue;
			};
			let Some((group, contribution)) = self.aggregator.extract(ctx, &row) else {
				continue;
			};
			let pane_start = bucket_of(coord, self.pane);
			let event = if is_add {
				AccumulatorEvent::Add(contribution)
			} else {
				AccumulatorEvent::Remove(contribution)
			};
			buckets.entry((group, pane_start)).or_default().push(event);
		}
	}
}

impl<A> TopKDriver<A>
where
	A: Emit + Send + Sync + 'static,
	A::Output: OutputRows + IntoIterator<Item = (Sk<A>, Rw<A>)>,
	A::GroupKey: Send + Sync,
	A::Accumulator: Send + Sync,
	Contribution<A>: Send + Sync,
	Sk<A>: Clone + Eq + Hash + Debug + StateCodec + HeapSize + Send + Sync,
	Rw<A>: Clone + Debug + PartialEq + StateCodec + HeapSize + Send + Sync,
	for<'a> &'a A::GroupKey: IntoEncodedKey,
	for<'a> &'a Sk<A>: IntoEncodedKey,
{
	fn expire_through<C: GuestContext>(
		engine: &mut TopKEngine<A>,
		store: &mut GuestAsHost<'_, C>,
		horizon: A::Coord,
	) -> Result<()> {
		if horizon > <A::Coord as Coord>::from_order(0) {
			engine.expire_meta(store, horizon.to_order())?;
		}
		Ok(())
	}

	fn combine(
		aggregator: &A,
		settings: &WindowSettings<A::Coord>,
		pane: <A::Coord as Coord>::Span,
		group: &A::GroupKey,
		buffer: &RollingBuffer<A::Coord, A::Accumulator>,
	) -> BTreeMap<Sk<A>, Rw<A>> {
		let Some(value) = <A::Kinds as KindSet<A>>::merge_panes(buffer).finalize() else {
			return BTreeMap::new();
		};
		let Some((newest, _)) = buffer.last_key_value() else {
			return BTreeMap::new();
		};
		let end = newest.add_span(pane);
		let span = WindowSpan::new(end.saturating_sub_span(settings.size), end);
		match aggregator.build_output(group, span, &value) {
			Some(rows) => rows.into_iter().collect(),
			None => BTreeMap::new(),
		}
	}

	fn emit_three_batches(
		ctx: &mut impl GuestContext,
		inserts: &[(RowNumber, Rw<A>)],
		updates: &[(RowNumber, Rw<A>, Rw<A>)],
		removes: &[(RowNumber, Rw<A>)],
	) -> Result<()> {
		if !inserts.is_empty() {
			let mut batch = InsertBatch::<Rw<A>, _>::new(ctx, inserts.len())?;
			for (rn, data) in inserts {
				batch.push(*rn, data)?;
			}
			batch.finish()?;
		}
		if !updates.is_empty() {
			let mut batch = UpdateBatch::<Rw<A>, _>::new(ctx, updates.len())?;
			for (rn, prior, new) in updates {
				batch.push(*rn, prior, new)?;
			}
			batch.finish()?;
		}
		if !removes.is_empty() {
			let mut batch = RemoveBatch::<Rw<A>, _>::new(ctx, removes.len())?;
			for (rn, data) in removes {
				batch.push(*rn, data)?;
			}
			batch.finish()?;
		}
		Ok(())
	}
}

impl<A> OperatorMetadata for TopKDriver<A>
where
	A: Emit + 'static,
	A::Output: OutputRows + IntoIterator<Item = (Sk<A>, Rw<A>)>,
	for<'a> &'a A::GroupKey: IntoEncodedKey,
	for<'a> &'a Sk<A>: IntoEncodedKey,
{
	const NAME: &'static str = A::NAME;
	const VERSION: &'static str = A::VERSION;
	const DESCRIPTION: &'static str = A::DESCRIPTION;
	const INPUT_COLUMNS: &'static [OperatorColumn] = A::INPUT_COLUMNS;
	const OUTPUT_COLUMNS: &'static [OperatorColumn] = A::OUTPUT_COLUMNS;
	const CAPABILITIES: &'static [OperatorCapability] = A::CAPABILITIES;
}

impl<A> MountedOperator for TopKDriver<A>
where
	A: Emit + Send + Sync + 'static,
	A::Output: OutputRows + IntoIterator<Item = (Sk<A>, Rw<A>)>,
	A::GroupKey: Send + Sync,
	A::Accumulator: Send + Sync,
	Contribution<A>: Send + Sync,
	Sk<A>: Clone + Eq + Hash + Debug + StateCodec + HeapSize + Send + Sync,
	Rw<A>: Clone + Debug + PartialEq + StateCodec + HeapSize + Send + Sync,
	for<'a> &'a A::GroupKey: IntoEncodedKey,
	for<'a> &'a Sk<A>: IntoEncodedKey,
{
	type Class = Windowed;

	fn sample(&self) -> Option<OperatorSample> {
		None
	}

	fn create(operator_id: OperatorId, params: &ExtensionParams, with: &ApplyWith) -> Result<Self> {
		with.require_window("rolling")?;
		let seal_span = <A::Coord as SealDomain>::seal_span_of(with)?;
		let settings = <A::Coord as SealDomain>::window_settings_of(with)?;
		let aggregator = A::create(operator_id, params, with)?;
		let Some(pane) = settings.pane else {
			return Err(Error::from(CoreError::OperatorWithPaneMissing).into());
		};
		Ok(Self {
			aggregator,
			engine: RollingTopKEngine::new(window_engine_config(params)),
			pane,
			seal_span,
			settings,
		})
	}

	fn on_timer(&mut self, ctx: &mut impl GuestContext, timer: Timer<'_>) -> Result<()> {
		let Some(seal_span) = self.seal_span else {
			return Ok(());
		};
		let mut store = GuestAsHost(ctx);
		let Some(frontier) = timer_frontier::<A::Coord>(&mut store, timer)? else {
			return Ok(());
		};
		let horizon = <A::Coord as SealDomain>::horizon(frontier, seal_span);
		Self::expire_through(&mut self.engine, &mut store, horizon)
	}

	fn apply(&mut self, ctx: &mut impl GuestContext, change: impl ChangeView) -> Result<()> {
		let mut buckets = self.route(ctx, &change);
		if buckets.is_empty() {
			return Ok(());
		}

		let seal_span = self.seal_span;
		if let Some(seal_span) = seal_span {
			let mut store = GuestAsHost(ctx);
			let newest = buckets.keys().map(|(_, coord)| *coord).max();
			if let Some(newest) = newest {
				observe_batch(&mut store, newest, seal_span)?;
			}
			let watermark = seal_frontier::<A::Coord>(&mut store)?;
			let horizon = <A::Coord as SealDomain>::horizon(watermark, seal_span);
			Self::expire_through(&mut self.engine, &mut store, horizon)?;
			let mut dropped = 0u64;
			buckets.retain(|(_, coord), events| {
				if is_sealed(*coord, horizon) {
					dropped += events.len() as u64;
					false
				} else {
					true
				}
			});
			if dropped > 0 {
				debug!(operator = A::NAME, dropped, "mutations targeting sealed panes were dropped");
			}
			if buckets.is_empty() {
				return Ok(());
			}
		}

		let emits = {
			let Self {
				aggregator,
				engine,
				pane,
				settings,
				..
			} = &mut *self;
			let pane = *pane;
			let mut store = GuestAsHost(ctx);
			engine.apply(
				&mut store,
				buckets,
				settings.size,
				|group| group.into_encoded_key(),
				|group, secondary| Self::row_key(group, secondary),
				|group, buffer| Self::combine(aggregator, settings, pane, group, buffer),
			)?
		};

		let mut inserts: Vec<(RowNumber, Rw<A>)> = Vec::new();
		let mut updates: Vec<(RowNumber, Rw<A>, Rw<A>)> = Vec::new();
		let mut removes: Vec<(RowNumber, Rw<A>)> = Vec::new();
		for emit in emits {
			match emit {
				TopKEmit::Insert {
					row_number,
					value,
				} => inserts.push((row_number, value)),
				TopKEmit::Update {
					row_number,
					prior,
					value,
				} => updates.push((row_number, prior, value)),
				TopKEmit::Remove {
					row_number,
					value,
				} => removes.push((row_number, value)),
			}
		}
		Self::emit_three_batches(ctx, &inserts, &updates, &removes)?;

		Ok(())
	}
}
