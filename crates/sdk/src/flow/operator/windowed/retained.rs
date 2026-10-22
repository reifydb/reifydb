// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::BTreeMap, fmt::Debug};

use reifydb_codec::{
	key::encoded::{EncodedKey, IntoEncodedKey},
	row::operator::state::{OperatorState, StateCodec},
};
use reifydb_core::{
	common::WindowRequirements,
	interface::{catalog::flow::OperatorId, flow::OperatorCapability},
	key::operator::state::GroupId,
	metrics::heap::{HeapSize, OperatorSample},
	operator_with::ApplyWith,
};
use reifydb_flow_async::{
	operator::{
		state::{
			reaper::{drain, drain_groups, enqueue},
			seal::{coord::Coord, domain::SealDomain, rule::is_sealed},
		},
		state_access::batch,
	},
	window::{
		accumulator::invertible::retained_map::RetainedAccumulator,
		engine::{
			AccumulatorEvent, EmitKind, PublishKey, WindowReindex,
			publish::PublishState,
			tumbling::TumblingBuckets,
			tumbling_retained::{RetainedEntry, RetainedTumblingEngine},
		},
		settings::WindowSettings,
		span::{WindowSpan, window_row_key},
	},
};
use reifydb_value::{
	config::ExtensionParams,
	reifydb_assertions,
	value::{diff_type::DiffType, row_number::RowNumber},
};
use tracing::debug;

use crate::{
	error::Result,
	flow::operator::{
		MountedOperator, OperatorMetadata,
		column::{operator::OperatorColumn, row::Row},
		context::{GuestContext, Windowed},
		timer::Timer,
		view::{ChangeView, ColumnsView, DiffView},
		windowed::{
			group_of,
			guest_as_host::GuestAsHost,
			intern_window_groups, observe_batch,
			operator::{Emit, WindowedOperator, extract_rows},
			publish::{Emitted, UpdateRow, publish_row},
			seal_frontier, timer_frontier, window_engine_config,
		},
	},
};

const SEAL_REAP_BATCH: usize = 256;

enum Decision {
	Remove,
	Due,
}

type SealSpan<A> = <<A as WindowedOperator>::Coord as SealDomain>::SealSpan;
type Buckets<A, K, V> = TumblingBuckets<<A as WindowedOperator>::GroupKey, <A as WindowedOperator>::Coord, (K, V)>;
type WindowOrder<A> = Vec<(<A as WindowedOperator>::GroupKey, WindowSpan<<A as WindowedOperator>::Coord>)>;
type Engine<A, K, V> = RetainedTumblingEngine<<A as WindowedOperator>::GroupKey, <A as WindowedOperator>::Coord, K, V>;

pub struct RetainedDriver<A, K, V>
where
	A: Emit<Accumulator = RetainedAccumulator<K, V>>,
	A::Output: Row,
	K: Ord,
	for<'a> &'a A::GroupKey: IntoEncodedKey,
{
	aggregator: A,
	engine: Engine<A, K, V>,
	reap_queue_empty: bool,
	seal_span: Option<SealSpan<A>>,
	settings: WindowSettings<A::Coord>,
}

impl<A, K, V> RetainedDriver<A, K, V>
where
	A: Emit<Accumulator = RetainedAccumulator<K, V>> + Send + Sync + 'static,
	A::Output: Row,
	A::Output: Clone + StateCodec + HeapSize,
	A::GroupKey: Send + Sync,
	K: Ord + Clone + Debug + HeapSize + StateCodec + Send + Sync,
	V: Clone + Debug + PartialEq + HeapSize + Send + Sync,
	RetainedEntry<K, V>: OperatorState,
	RetainedAccumulator<K, V>: OperatorState + StateCodec + HeapSize,
	for<'a> &'a A::GroupKey: IntoEncodedKey,
{
	fn window_span(&self, coord: A::Coord) -> WindowSpan<A::Coord> {
		WindowSpan::for_coord(coord, self.settings.fixed_size())
	}

	fn row_key(group: &A::GroupKey, window_start: A::Coord) -> EncodedKey {
		window_row_key(group.into_encoded_key(), window_start)
	}

	fn route(&self, ctx: &mut impl GuestContext, change: &impl ChangeView) -> Result<Buckets<A, K, V>> {
		let mut buckets: Buckets<A, K, V> = BTreeMap::new();

		for di in 0..change.diff_count() {
			let Some(diff) = change.diff(di) else {
				continue;
			};
			match diff.kind() {
				DiffType::Insert => {
					if let Some(cols) = diff.post() {
						self.push_all(ctx, &cols, &mut buckets, true)?;
					}
				}
				DiffType::Update => {
					if let (Some(pre), Some(post)) = (diff.pre(), diff.post()) {
						self.push_all(ctx, &pre, &mut buckets, false)?;
						self.push_all(ctx, &post, &mut buckets, true)?;
					}
				}
				DiffType::Remove => {
					if let Some(cols) = diff.pre() {
						self.push_all(ctx, &cols, &mut buckets, false)?;
					}
				}
			}
		}
		Ok(buckets)
	}

	fn push_all<C: ColumnsView>(
		&self,
		ctx: &mut impl GuestContext,
		cols: &C,
		buckets: &mut Buckets<A, K, V>,
		is_add: bool,
	) -> Result<()> {
		let rows: Vec<usize> = (0..cols.row_count()).collect();
		for (_, coord, group, contribution) in extract_rows(&self.aggregator, ctx, cols, &rows)? {
			let span = self.window_span(coord);
			let event = if is_add {
				AccumulatorEvent::Add(contribution)
			} else {
				AccumulatorEvent::Remove(contribution)
			};
			buckets.entry((group, span)).or_default().push(event);
		}
		Ok(())
	}

	#[allow(clippy::too_many_arguments)]
	fn expire_through<C: GuestContext>(
		engine: &mut Engine<A, K, V>,
		reap_queue_empty: &mut bool,
		store: &mut GuestAsHost<'_, C>,
		frontier: A::Coord,
		seal_span: SealSpan<A>,
	) -> Result<()> {
		let horizon = <A::Coord as SealDomain>::horizon(frontier, seal_span);
		if horizon <= <A::Coord as Coord>::from_order(0) {
			return Ok(());
		}
		let store_queue_empty = *reap_queue_empty;
		let closing = engine.expire(store, horizon.to_order().saturating_sub(1))?;
		let expired: Vec<GroupId> = closing.iter().map(|window| window.group_id).collect();
		enqueue(store, &expired)?;
		if !expired.is_empty() {
			*reap_queue_empty = false;
		}
		engine.expire_meta(store, horizon.to_order())?;
		if *reap_queue_empty {
			reifydb_assertions! {
				let pending = reifydb_flow_async::operator::state::reaper::queued(store, 1)?;
				assert!(
					pending.groups.is_empty(),
					"the reap queue still holds {:?} while the driver believes it drained the \
					 queue; skipping the drain here leaves that group's state behind for good",
					pending.groups
				);
			}
			return Ok(());
		}
		let drained = match store_queue_empty {
			true => drain_groups(store, &expired, engine, SEAL_REAP_BATCH)?,
			false => drain(store, engine, SEAL_REAP_BATCH)?,
		};
		*reap_queue_empty = drained.queue_is_empty();
		if !*reap_queue_empty {
			observe_batch(store, frontier, seal_span)?;
		}
		Ok(())
	}
	#[allow(clippy::too_many_arguments)]
	fn apply_tumbling<C: GuestContext>(
		aggregator: &A,
		engine: &mut Engine<A, K, V>,
		reap_queue_empty: &mut bool,
		seal_span: Option<SealSpan<A>>,
		ctx: &mut C,
		mut buckets: Buckets<A, K, V>,
	) -> Result<Emitted<A>> {
		let mut emitted: Emitted<A> = (Vec::new(), Vec::new(), Vec::new());
		if let Some(seal_span) = seal_span {
			let mut store = GuestAsHost(ctx);
			let newest = buckets.keys().map(|(_, span)| span.start).max();
			if let Some(newest) = newest {
				observe_batch(&mut store, newest, seal_span)?;
			}
			let watermark = seal_frontier::<A::Coord>(&mut store)?;
			let horizon = <A::Coord as SealDomain>::horizon(watermark, seal_span);
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
			Self::expire_through(engine, reap_queue_empty, &mut store, watermark, seal_span)?;
			if buckets.is_empty() {
				return Ok(emitted);
			}
		}

		let groups = intern_window_groups(
			buckets.keys()
				.map(|(group, span)| ((group.clone(), span.start), Self::row_key(group, span.start))),
		);

		let results = {
			let mut store = GuestAsHost(ctx);
			let order: WindowOrder<A> = buckets.keys().cloned().collect();
			engine.apply(&mut store, buckets, &order, |group, window_start| {
				group_of(&groups, group, window_start)
			})?
		};

		if seal_span.is_some() {
			let inserted: Vec<_> = results
				.iter()
				.filter(|r| r.kind == EmitKind::Insert)
				.map(|r| (r, group_of(&groups, &r.group, r.span.start).1))
				.collect();
			let entries: Vec<_> = inserted
				.iter()
				.map(|(r, key)| WindowReindex {
					group: &r.group,
					window_start: r.span.start,
					id: r.group_id,
					slot_key: key,
					prior: None,
					new: Some(r.span.start.to_order()),
				})
				.collect();
			engine.reindex_windows(&mut GuestAsHost(ctx), &entries)?;
		}

		if results.is_empty() {
			return Ok(emitted);
		}
		let mut store = GuestAsHost(ctx);
		let keys: Vec<PublishKey> = results
			.iter()
			.map(|r| {
				let (_, key) = group_of(&groups, &r.group, r.span.start);
				PublishKey::new(r.group_id, key)
			})
			.collect();
		let mut session = batch::<_, PublishState<A::Output>>(&mut store, &keys)?;
		let mut decided = Vec::with_capacity(results.len());
		let mut due_windows: Vec<(GroupId, u64)> = Vec::new();
		for (slot, r) in results.into_iter().enumerate() {
			let state = session.value(slot)?.unwrap_or_default();
			let decision = if r.kind == EmitKind::Remove {
				Decision::Remove
			} else {
				due_windows.push((r.group_id, r.count));
				Decision::Due
			};
			decided.push((slot, r, state, decision));
		}
		let mut read = engine.load_entries(&mut store, &due_windows)?;
		for (slot, r, mut state, decision) in decided {
			match decision {
				Decision::Remove => publish_row::<A>(r.row_number, None, &mut state, &mut emitted)?,
				Decision::Due => {
					let entries = read
						.remove(&r.group_id)
						.expect("the entry read answers every due window");
					let out = aggregator.build_output(&r.group, r.span, &entries);
					publish_row::<A>(r.row_number, out, &mut state, &mut emitted)?;
				}
			}
			session.set(slot, &state)?;
		}
		session.commit(&mut store)?;
		Ok(emitted)
	}

	#[inline]
	fn emit_batches(
		&self,
		ctx: &mut impl GuestContext,
		inserts: &[(RowNumber, A::Output)],
		updates: &[UpdateRow<A>],
		removes: &[(RowNumber, A::Output)],
	) -> Result<()> {
		let row_numbers: Vec<RowNumber> = inserts.iter().map(|(rn, _)| *rn).collect();
		ctx.emit_insert(inserts.iter().map(|(_, data)| data), &row_numbers)?;
		let row_numbers: Vec<RowNumber> = updates.iter().map(|(rn, ..)| *rn).collect();
		ctx.emit_update(
			updates.iter().map(|(_, pre, post)| pre.as_ref().unwrap_or(post)),
			updates.iter().map(|(_, _, post)| post),
			&row_numbers,
		)?;
		let row_numbers: Vec<RowNumber> = removes.iter().map(|(rn, _)| *rn).collect();
		ctx.emit_remove(removes.iter().map(|(_, data)| data), &row_numbers)?;
		Ok(())
	}
}

impl<A, K, V> OperatorMetadata for RetainedDriver<A, K, V>
where
	A: Emit<Accumulator = RetainedAccumulator<K, V>> + 'static,
	A::Output: Row,
	K: Ord,
	for<'a> &'a A::GroupKey: IntoEncodedKey,
{
	const NAME: &'static str = A::NAME;
	const VERSION: &'static str = A::VERSION;
	const DESCRIPTION: &'static str = A::DESCRIPTION;
	const INPUT_COLUMNS: &'static [OperatorColumn] = A::INPUT_COLUMNS;
	const OUTPUT_COLUMNS: &'static [OperatorColumn] = A::OUTPUT_COLUMNS;
	const CAPABILITIES: &'static [OperatorCapability] = A::CAPABILITIES;
}

impl<A, K, V> MountedOperator for RetainedDriver<A, K, V>
where
	A: Emit<Accumulator = RetainedAccumulator<K, V>> + Send + Sync + 'static,
	A::Output: Row,
	A::Output: Clone + StateCodec + HeapSize,
	A::GroupKey: Send + Sync,
	K: Ord + Clone + Debug + HeapSize + StateCodec + Send + Sync + 'static,
	V: Clone + Debug + PartialEq + HeapSize + Send + Sync + 'static,
	RetainedEntry<K, V>: OperatorState,
	RetainedAccumulator<K, V>: OperatorState + StateCodec + HeapSize,
	for<'a> &'a A::GroupKey: IntoEncodedKey,
{
	type Class = Windowed;

	const WINDOW: WindowRequirements = WindowRequirements {
		takes_window: true,
		kinds: &["tumbling"],
		domain: <A::Coord as SealDomain>::SIZE_DOMAIN,
		needs_pane: false,
	};

	const UNMANAGED_BECAUSE: Option<&'static str> = None;

	fn sample(&self) -> Option<OperatorSample> {
		None
	}

	fn create(operator_id: OperatorId, params: &ExtensionParams, with: &ApplyWith) -> Result<Self> {
		with.reject_retention()?;
		with.require_window("tumbling")?;
		let seal_span = <A::Coord as SealDomain>::seal_span_of(with)?;
		let settings = <A::Coord as SealDomain>::window_settings_of(with)?;
		let aggregator = A::create(operator_id, params, with)?;
		Ok(Self {
			aggregator,
			engine: RetainedTumblingEngine::new(window_engine_config(params)),
			reap_queue_empty: false,
			seal_span,
			settings,
		})
	}

	fn on_timer(&mut self, ctx: &mut impl GuestContext, timer: Timer<'_>) -> Result<()> {
		let Some(seal_span) = self.seal_span else {
			return Ok(());
		};
		let Self {
			engine,
			reap_queue_empty,
			..
		} = &mut *self;
		let mut store = GuestAsHost(ctx);
		let Some(frontier) = timer_frontier::<A::Coord>(&mut store, timer)? else {
			return Ok(());
		};
		Self::expire_through(engine, reap_queue_empty, &mut store, frontier, seal_span)
	}

	fn apply(&mut self, ctx: &mut impl GuestContext, change: impl ChangeView) -> Result<()> {
		let buckets = self.route(ctx, &change)?;
		if buckets.is_empty() {
			return Ok(());
		}
		let (inserts, updates, removes) = {
			let Self {
				aggregator,
				engine,
				reap_queue_empty,
				seal_span,
				..
			} = &mut *self;
			Self::apply_tumbling(aggregator, engine, reap_queue_empty, *seal_span, ctx, buckets)?
		};
		self.emit_batches(ctx, &inserts, &updates, &removes)
	}
}

#[cfg(test)]
mod tests {
	use std::marker::PhantomData;

	use reifydb_core::common::{WindowKind, WindowSize};
	use reifydb_value::value::{datetime::DateTime, duration::Duration};

	use super::*;
	use crate::row;

	#[reifydb_macro::operator_state]
	#[derive(Clone, HeapSize)]
	struct Out {
		v: i64,
	}

	row!(Out {
		v: i64
	});

	macro_rules! probe {
		($name:ident, $coord:ty) => {
			struct $name(PhantomData<$coord>);

			impl OperatorMetadata for $name {
				const NAME: &'static str = "probe";
				const VERSION: &'static str = "0";
				const DESCRIPTION: &'static str = "probe";
				const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
				const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
				const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
			}

			impl WindowedOperator for $name {
				type Coord = $coord;
				type GroupKey = u64;
				type Accumulator = RetainedAccumulator<u64, i64>;
				type Output = Out;

				fn create(_: OperatorId, _: &ExtensionParams, _: &ApplyWith) -> Result<Self> {
					Ok(Self(PhantomData))
				}

				fn coord(
					&self,
					cols: &impl crate::flow::operator::view::ColumnsView,
					rows: &[usize],
				) -> Result<Vec<Option<$coord>>> {
					rows.iter()
						.map(|&i| {
							cols.row(i)
								.as_ref()
								.map(|_| -> Result<Option<$coord>> { Ok(None) })
								.transpose()
								.map(Option::flatten)
						})
						.collect()
				}

				fn extract(
					&self,
					_: &mut impl GuestContext<Windowed>,
					cols: &impl crate::flow::operator::view::ColumnsView,
					rows: &[usize],
				) -> Result<Vec<Option<(u64, (u64, i64))>>> {
					rows.iter()
						.map(|&i| {
							cols.row(i)
								.as_ref()
								.map(|_| -> Result<Option<(u64, (u64, i64))>> {
									Ok(None)
								})
								.transpose()
								.map(Option::flatten)
						})
						.collect()
				}

				fn new_accumulator(&self, _: &WindowSettings<$coord>) -> RetainedAccumulator<u64, i64> {
					RetainedAccumulator::default()
				}
			}

			impl Emit for $name {
				type Kinds = crate::flow::operator::windowed::operator::NoRolling;

				fn build_output(
					&self,
					_: &u64,
					_: WindowSpan<$coord>,
					_: &BTreeMap<u64, i64>,
				) -> Option<Out> {
					None
				}
			}
		};
	}

	probe!(TimeProbe, DateTime);

	fn params() -> ExtensionParams {
		ExtensionParams::new("probe", Default::default())
	}

	#[test]
	fn create_refuses_a_window_that_is_not_tumbling() {
		// Only the tumbling path reads entries; a sliding window accepted here would publish from the wrong
		// spans.
		let tumbling = ApplyWith {
			window: Some(WindowKind::Tumbling {
				size: WindowSize::Duration(Duration::from_seconds(60).unwrap()),
			}),
			..ApplyWith::default()
		};
		let sliding = ApplyWith {
			window: Some(WindowKind::Sliding {
				size: WindowSize::Duration(Duration::from_seconds(60).unwrap()),
				slide: WindowSize::Duration(Duration::from_seconds(10).unwrap()),
			}),
			..ApplyWith::default()
		};
		assert!(
			RetainedDriver::<TimeProbe, u64, i64>::create(OperatorId(1), &params(), &tumbling).is_ok(),
			"precondition"
		);
		assert!(RetainedDriver::<TimeProbe, u64, i64>::create(OperatorId(1), &params(), &sliding).is_err());
	}
}
