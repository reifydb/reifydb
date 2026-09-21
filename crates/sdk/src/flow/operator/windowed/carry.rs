// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::BTreeMap, fmt::Debug};

use reifydb_codec::{
	key::encoded::{EncodedKey, IntoEncodedKey},
	row::operator::state::StateCodec,
};
use reifydb_core::{
	common::WindowRequirements,
	interface::{catalog::flow::OperatorId, flow::OperatorCapability},
	metrics::heap::{HeapSize, OperatorSample},
	operator_with::ApplyWith,
};
use reifydb_flow::{
	operator::state::seal::{coord::Coord, domain::SealDomain, rule::is_sealed},
	window::{
		engine::{
			AccumulatorEvent, EmitKind, WindowResult, config::TumblingCarryConfig,
			tumbling::TumblingBuckets, tumbling_carry::TumblingCarryEngine,
		},
		settings::WindowSettings,
		span::{WindowSpan, window_row_key},
	},
};
use reifydb_value::{
	config::ExtensionParams,
	value::{diff_type::DiffType, row_number::RowNumber},
};
use tracing::{debug, instrument};

use crate::{
	error::Result,
	flow::operator::{
		MountedOperator, OperatorMetadata,
		column::{
			batch::{InsertBatch, RemoveBatch, UpdateBatch},
			operator::OperatorColumn,
			row::Row,
		},
		context::{GuestContext, Windowed},
		timer::Timer,
		view::{ChangeView, ColumnsView, DiffView},
		windowed::{
			guest_as_host::GuestAsHost,
			observe_batch,
			operator::{CarryEmit, Contribution, WindowedOperator},
			seal_frontier, timer_frontier, window_engine_config,
		},
	},
};

type SealSpan<A> = <<A as WindowedOperator>::Coord as SealDomain>::SealSpan;
type CarryEngine<A> = TumblingCarryEngine<
	<A as WindowedOperator>::GroupKey,
	<A as WindowedOperator>::Coord,
	<A as WindowedOperator>::Accumulator,
	<A as CarryEmit>::Carry,
	<A as WindowedOperator>::Output,
>;
type Buckets<A> = TumblingBuckets<<A as WindowedOperator>::GroupKey, <A as WindowedOperator>::Coord, Contribution<A>>;
type WindowResults<A> = Vec<
	WindowResult<
		<A as WindowedOperator>::GroupKey,
		<A as WindowedOperator>::Coord,
		<A as WindowedOperator>::Output,
	>,
>;

pub struct CarryDriver<A>
where
	A: CarryEmit,
	A::Output: Row,
	for<'a> &'a A::GroupKey: IntoEncodedKey,
{
	aggregator: A,
	engine: CarryEngine<A>,
	seal_span: Option<SealSpan<A>>,
	settings: WindowSettings<A::Coord>,
}

impl<A> CarryDriver<A>
where
	A: CarryEmit,
	A::Output: Row,
	for<'a> &'a A::GroupKey: IntoEncodedKey,
{
	fn window_span(&self, coord: A::Coord) -> WindowSpan<A::Coord> {
		WindowSpan::for_coord(coord, self.settings.size)
	}

	fn row_key(group: &A::GroupKey, window_start: A::Coord) -> EncodedKey {
		window_row_key(group.into_encoded_key(), window_start)
	}

	#[instrument(name = "flow::operator::tumbling::route", level = "trace", skip_all, fields(operator = A::NAME))]
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
			let span = self.window_span(coord);
			let event = if is_add {
				AccumulatorEvent::Add(contribution)
			} else {
				AccumulatorEvent::Remove(contribution)
			};
			buckets.entry((group, span)).or_default().push(event);
		}
	}

	#[inline]
	#[instrument(name = "flow::operator::tumbling::emit", level = "trace", skip_all, fields(operator = A::NAME))]
	fn emit_batches(
		&self,
		ctx: &mut impl GuestContext,
		inserts: &[(RowNumber, A::Output)],
		updates: &[(RowNumber, A::Output)],
		removes: &[(RowNumber, A::Output)],
	) -> Result<()> {
		if !inserts.is_empty() {
			let mut batch = InsertBatch::<A::Output, _>::new(ctx, inserts.len())?;
			for (rn, data) in inserts {
				batch.push(*rn, data)?;
			}
			batch.finish()?;
		}
		if !updates.is_empty() {
			let mut batch = UpdateBatch::<A::Output, _>::new(ctx, updates.len())?;
			for (rn, data) in updates {
				batch.push(*rn, data, data)?;
			}
			batch.finish()?;
		}
		if !removes.is_empty() {
			let mut batch = RemoveBatch::<A::Output, _>::new(ctx, removes.len())?;
			for (rn, data) in removes {
				batch.push(*rn, data)?;
			}
			batch.finish()?;
		}
		Ok(())
	}
}

impl<A> CarryDriver<A>
where
	A: CarryEmit + Send + Sync + 'static,
	A::Output: Row,
	A::GroupKey: Send + Sync,
	A::Accumulator: Send + Sync + HeapSize,
	A::Carry: Send + Sync + HeapSize,
	A::Output: Clone + Debug + StateCodec + Send + Sync + HeapSize,
	Contribution<A>: Send + Sync,
	for<'a> &'a A::GroupKey: IntoEncodedKey,
{
	fn expire_through<C: GuestContext>(
		engine: &mut CarryEngine<A>,
		store: &mut GuestAsHost<'_, C>,
		horizon: A::Coord,
	) -> Result<()> {
		if horizon > <A::Coord as Coord>::from_order(0) {
			engine.expire(store, horizon, |group, window_start| Self::row_key(group, window_start))?;
		}
		Ok(())
	}

	#[instrument(name = "flow::operator::tumbling::seal", level = "trace", skip_all, fields(operator = A::NAME))]
	fn seal(
		&mut self,
		ctx: &mut impl GuestContext,
		buckets: &mut Buckets<A>,
		seal_span: SealSpan<A>,
	) -> Result<()> {
		let mut store = GuestAsHost(ctx);
		let newest = buckets.keys().map(|(_, span)| span.start).max();
		if let Some(newest) = newest {
			observe_batch(&mut store, newest, seal_span)?;
		}
		let watermark = seal_frontier::<A::Coord>(&mut store)?;
		let horizon = <A::Coord as SealDomain>::horizon(watermark, seal_span);
		Self::expire_through(&mut self.engine, &mut store, horizon)?;
		let mut dropped = 0u64;
		buckets.retain(|(_, span), events| {
			if is_sealed(span.start, horizon) {
				dropped += events.len() as u64;
				false
			} else {
				true
			}
		});
		if dropped > 0 {
			debug!(operator = A::NAME, dropped, "mutations targeting sealed windows were dropped");
		}
		Ok(())
	}

	#[instrument(name = "flow::operator::tumbling::accumulate", level = "trace", skip_all, fields(operator = A::NAME))]
	fn accumulate(&mut self, ctx: &mut impl GuestContext, buckets: Buckets<A>) -> Result<WindowResults<A>> {
		let Self {
			aggregator,
			engine,
			settings,
			..
		} = &mut *self;
		let mut store = GuestAsHost(ctx);
		Ok(engine.apply(
			&mut store,
			buckets,
			|group, window_start| Self::row_key(group, window_start),
			|| aggregator.new_accumulator(settings),
			|group, span, value, prev_carry| aggregator.build_output(group, span, value, prev_carry),
			|value, prev_carry| aggregator.carry_forward(value, prev_carry),
		)?)
	}
}

impl<A> OperatorMetadata for CarryDriver<A>
where
	A: CarryEmit + 'static,
	A::Output: Row,
	for<'a> &'a A::GroupKey: IntoEncodedKey,
{
	const NAME: &'static str = A::NAME;
	const VERSION: &'static str = A::VERSION;
	const DESCRIPTION: &'static str = A::DESCRIPTION;
	const INPUT_COLUMNS: &'static [OperatorColumn] = A::INPUT_COLUMNS;
	const OUTPUT_COLUMNS: &'static [OperatorColumn] = A::OUTPUT_COLUMNS;
	const CAPABILITIES: &'static [OperatorCapability] = A::CAPABILITIES;
}

impl<A> MountedOperator for CarryDriver<A>
where
	A: CarryEmit + Send + Sync + 'static,
	A::Output: Row,
	A::GroupKey: Send + Sync,
	A::Accumulator: Send + Sync + HeapSize,
	A::Carry: Send + Sync + HeapSize,
	A::Output: Clone + Debug + StateCodec + Send + Sync + HeapSize,
	Contribution<A>: Send + Sync,
	for<'a> &'a A::GroupKey: IntoEncodedKey,
{
	type Class = Windowed;

	const WINDOW: WindowRequirements = WindowRequirements {
		takes_window: true,
		kinds: &["tumbling"],
		domain: <A::Coord as SealDomain>::SIZE_DOMAIN,
		needs_pane: false,
	};

	fn sample(&self) -> Option<OperatorSample> {
		None
	}

	fn create(operator_id: OperatorId, params: &ExtensionParams, with: &ApplyWith) -> Result<Self> {
		with.require_window("tumbling")?;
		let seal_span = <A::Coord as SealDomain>::seal_span_of(with)?;
		let settings = <A::Coord as SealDomain>::window_settings_of(with)?;
		let aggregator = A::create(operator_id, params, with)?;
		let engine_config = window_engine_config(params);
		Ok(Self {
			aggregator,
			engine: TumblingCarryEngine::new(
				TumblingCarryConfig::builder(engine_config).retention(settings.immutable).build(),
			),
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
			self.seal(ctx, &mut buckets, seal_span)?;
			if buckets.is_empty() {
				return Ok(());
			}
		}

		let results = self.accumulate(ctx, buckets)?;

		let mut inserts: Vec<(RowNumber, A::Output)> = Vec::new();
		let mut updates: Vec<(RowNumber, A::Output)> = Vec::new();
		let mut removes: Vec<(RowNumber, A::Output)> = Vec::new();
		for r in results {
			match r.kind {
				EmitKind::Insert => inserts.push((r.row_number, r.value)),
				EmitKind::Update => updates.push((r.row_number, r.value)),
				EmitKind::Remove => removes.push((r.row_number, r.value)),
			}
		}
		self.emit_batches(ctx, &inserts, &updates, &removes)?;

		Ok(())
	}
}
