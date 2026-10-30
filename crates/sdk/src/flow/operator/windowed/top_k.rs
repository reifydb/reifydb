// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::BTreeMap, fmt::Debug, hash::Hash};

use reifydb_codec::{
	key::encoded::{EncodedKey, IntoEncodedKey},
	row::operator::state::StateCodec,
};
use reifydb_core::{
	common::WindowRequirements,
	error::CoreError,
	interface::{catalog::flow::OperatorId, flow::OperatorCapability},
	key::operator::state::GroupId,
	metrics::heap::{HeapSize, OperatorSample},
	operator_with::ApplyWith,
	state::timer::StateStore,
};
use reifydb_flow_async::{
	operator::state::seal::{coord::Coord, domain::SealDomain},
	window::{
		engine::{
			daba::Merge,
			pane::{PaneEngine, PaneEvent, PanePublisher, PaneSpans},
		},
		prefetch::Prefetched,
		settings::WindowSettings,
	},
};
use reifydb_value::{Result as ValueResult, config::ExtensionParams, error::Error, value::row_number::RowNumber};
use tracing::debug;

use crate::{
	error::Result,
	flow::operator::{
		MountedOperator, OperatorMetadata,
		column::{operator::OperatorColumn, row::OutputRows},
		context::{GuestContext, Windowed},
		timer::Timer,
		view::ChangeView,
		windowed::{
			RollingGroups,
			guest_as_host::GuestAsHost,
			observe_batch,
			operator::{Contribution, Emit, KindSet, WindowedOperator},
			rolling_cutoff, rolling_output, route_rolling, seal_frontier, timer_frontier,
			window_engine_config,
		},
	},
};

type SealSpan<A> = <<A as WindowedOperator>::Coord as SealDomain>::SealSpan;
type Sk<A> = <<A as WindowedOperator>::Output as OutputRows>::Key;
type Rw<A> = <<A as WindowedOperator>::Output as OutputRows>::Row;
type Ranking<A> = BTreeMap<Sk<A>, (RowNumber, Rw<A>)>;

pub(crate) enum TopKRow<R> {
	Insert(RowNumber, R),
	Update(RowNumber, R, R),
	Remove(RowNumber, R),
}

pub(crate) struct TopKPublisher<'f, A: Emit> {
	aggregator: &'f A,
	settings: &'f WindowSettings<A::Coord>,
	pane: <A::Coord as Coord>::Span,
}

impl<A> PanePublisher<A::GroupKey, A::Coord, A::Accumulator> for TopKPublisher<'_, A>
where
	A: Emit,
	A::Output: OutputRows + IntoIterator<Item = (Sk<A>, Rw<A>)>,
	Sk<A>: Clone + Ord,
	Rw<A>: Clone + PartialEq,
	for<'a> &'a A::GroupKey: IntoEncodedKey,
	for<'a> &'a Sk<A>: IntoEncodedKey,
{
	type Published = Ranking<A>;
	type Emit = TopKRow<Rw<A>>;

	fn publish(
		&self,
		store: &mut dyn StateStore,
		group: &A::GroupKey,
		group_id: GroupId,
		newest: A::Coord,
		prior: Option<Ranking<A>>,
		value: Option<&A::Accumulator>,
		emits: &mut Vec<TopKRow<Rw<A>>>,
	) -> ValueResult<Option<Ranking<A>>> {
		let prior = prior.unwrap_or_default();
		let ranking: BTreeMap<Sk<A>, Rw<A>> = match value {
			Some(value) => rolling_output(self.aggregator, self.settings, self.pane, group, newest, value)
				.map(|rows| rows.into_iter().collect())
				.unwrap_or_default(),
			None => BTreeMap::new(),
		};
		let added: Vec<&Sk<A>> = ranking.keys().filter(|sk| !prior.contains_key(*sk)).collect();
		let added_keys: Vec<EncodedKey> = added.iter().map(|sk| row_key(group, *sk)).collect();
		let numbers = store.get_or_create_row_numbers(group_id, &added_keys)?;
		let mut next: Ranking<A> = BTreeMap::new();
		let mut freed: Vec<EncodedKey> = Vec::new();
		for (sk, (number, old)) in prior {
			match ranking.get(&sk) {
				Some(new) => {
					if *new != old {
						emits.push(TopKRow::Update(number, old, new.clone()));
					}
					next.insert(sk, (number, new.clone()));
				}
				None => {
					freed.push(row_key(group, &sk));
					emits.push(TopKRow::Remove(number, old));
				}
			}
		}
		for (sk, (number, is_new)) in added.into_iter().zip(numbers) {
			let new = ranking[sk].clone();
			emits.push(if is_new {
				TopKRow::Insert(number, new.clone())
			} else {
				TopKRow::Update(number, new.clone(), new.clone())
			});
			next.insert(sk.clone(), (number, new));
		}
		if !freed.is_empty() {
			store.remove_row_number(group_id, &freed)?;
		}
		Ok((!next.is_empty()).then_some(next))
	}
}

fn row_key<G, S>(group: &G, secondary: &S) -> EncodedKey
where
	for<'a> &'a G: IntoEncodedKey,
	for<'a> &'a S: IntoEncodedKey,
{
	EncodedKey::builder()
		.raw(group.into_encoded_key().as_slice())
		.raw(secondary.into_encoded_key().as_slice())
		.build()
}

pub struct TopKDriver<A>
where
	A: Emit,
	A::Output: OutputRows + IntoIterator<Item = (Sk<A>, Rw<A>)>,
	for<'a> &'a A::GroupKey: IntoEncodedKey,
	for<'a> &'a Sk<A>: IntoEncodedKey,
{
	aggregator: A,
	engine: PaneEngine<A::GroupKey, A::Coord, A::Accumulator>,
	pane: <A::Coord as Coord>::Span,
	seal_span: Option<SealSpan<A>>,
	settings: WindowSettings<A::Coord>,
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
	fn apply_rolling(
		&mut self,
		ctx: &mut impl GuestContext,
		groups: RollingGroups<A>,
	) -> Result<Vec<TopKRow<Rw<A>>>> {
		let mut store = GuestAsHost(ctx);
		let newest = groups.values().flat_map(|(_, events)| events.iter().flat_map(PaneEvent::panes)).max();
		if let (Some(seal_span), Some(newest)) = (self.seal_span, newest) {
			observe_batch(&mut store, newest, seal_span)?;
		}
		let frontier = seal_frontier::<A::Coord>(&mut store)?;
		let clock = match (self.seal_span, newest) {
			(None, Some(newest)) => frontier.max(newest),
			_ => frontier,
		};
		let Self {
			aggregator,
			engine,
			pane,
			settings,
			..
		} = &mut *self;
		let cutoff = rolling_cutoff::<A>(settings, *pane, clock);
		let publisher = TopKPublisher {
			aggregator: &*aggregator,
			settings: &*settings,
			pane: *pane,
		};
		let before = engine.earliest_expiry(&mut store)?;
		let mut emits = match cutoff {
			Some(cutoff) => engine.expire(&mut store, cutoff, &publisher)?,
			None => Vec::new(),
		};
		let keys = engine.state_keys(&groups);
		let mut prefetched = Prefetched::read(&mut store, keys)?;
		let applied = engine.apply(
			&mut prefetched,
			groups,
			cutoff,
			&|| aggregator.new_accumulator(settings),
			&publisher,
		)?;
		prefetched.write()?;
		if applied.sealed_drops + applied.stale_retractions > 0 {
			debug!(
				operator = A::NAME,
				sealed_drops = applied.sealed_drops,
				stale_retractions = applied.stale_retractions,
				"rolling input dropped past its group's seal or retract line"
			);
		}
		emits.extend(applied.emits);
		let after = engine.earliest_expiry(&mut store)?;
		<A::Coord as SealDomain>::rearm_decay(&mut store, settings.fixed_size(), before, after)?;
		Ok(emits)
	}

	fn emit_rows(ctx: &mut impl GuestContext, rows: Vec<TopKRow<Rw<A>>>) -> Result<()> {
		let mut inserts: Vec<(RowNumber, Rw<A>)> = Vec::new();
		let mut updates: Vec<(RowNumber, Rw<A>, Rw<A>)> = Vec::new();
		let mut removes: Vec<(RowNumber, Rw<A>)> = Vec::new();
		for row in rows {
			match row {
				TopKRow::Insert(number, value) => inserts.push((number, value)),
				TopKRow::Update(number, prior, value) => updates.push((number, prior, value)),
				TopKRow::Remove(number, value) => removes.push((number, value)),
			}
		}
		let row_numbers: Vec<RowNumber> = inserts.iter().map(|(rn, _)| *rn).collect();
		ctx.emit_insert(inserts.iter().map(|(_, data)| data), &row_numbers)?;
		let row_numbers: Vec<RowNumber> = updates.iter().map(|(rn, ..)| *rn).collect();
		ctx.emit_update(
			updates.iter().map(|(_, prior, _)| prior),
			updates.iter().map(|(_, _, new)| new),
			&row_numbers,
		)?;
		let row_numbers: Vec<RowNumber> = removes.iter().map(|(rn, _)| *rn).collect();
		ctx.emit_remove(removes.iter().map(|(_, data)| data), &row_numbers)?;
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

	const WINDOW: WindowRequirements = WindowRequirements {
		takes_window: true,
		kinds: &["rolling"],
		domain: <A::Coord as SealDomain>::SIZE_DOMAIN,
		needs_pane: true,
	};

	const UNMANAGED_BECAUSE: Option<&'static str> = None;

	fn sample(&self) -> Option<OperatorSample> {
		None
	}

	fn create(operator_id: OperatorId, params: &ExtensionParams, with: &ApplyWith) -> Result<Self> {
		with.reject_retention()?;
		with.require_window("rolling")?;
		let seal_span = <A::Coord as SealDomain>::seal_span_of(with)?;
		let settings = <A::Coord as SealDomain>::window_settings_of(with)?;
		let aggregator = A::create(operator_id, params, with)?;
		let Some(pane) = settings.pane else {
			return Err(Error::from(CoreError::OperatorWithPaneMissing).into());
		};
		if !<A::Kinds as KindSet<A>>::compacts(&aggregator.new_accumulator(&settings)) {
			return Err(Error::from(CoreError::OperatorRollingUnbounded {
				operator: A::NAME,
			})
			.into());
		}
		let merge = Merge {
			merge: <A::Kinds as KindSet<A>>::merge,
			compact: <A::Kinds as KindSet<A>>::compact,
		};
		let spans = PaneSpans {
			size: settings.fixed_size(),
			lag: Default::default(),
			lateness: settings.lateness,
			retract: settings.immutable.unwrap_or(settings.lateness),
		};
		Ok(Self {
			aggregator,
			engine: PaneEngine::queue(window_engine_config(params), spans, merge),
			pane,
			seal_span,
			settings,
		})
	}

	fn on_timer(&mut self, ctx: &mut impl GuestContext, timer: Timer<'_>) -> Result<()> {
		if self.seal_span.is_none() {
			return Ok(());
		}
		let Self {
			aggregator,
			engine,
			pane,
			settings,
			..
		} = &mut *self;
		let rows = {
			let mut store = GuestAsHost(ctx);
			let Some(frontier) = timer_frontier::<A::Coord>(&mut store, timer)? else {
				return Ok(());
			};
			let before = engine.earliest_expiry(&mut store)?;
			let publisher = TopKPublisher {
				aggregator: &*aggregator,
				settings: &*settings,
				pane: *pane,
			};
			let rows = match rolling_cutoff::<A>(settings, *pane, frontier) {
				Some(cutoff) => engine.expire(&mut store, cutoff, &publisher)?,
				None => Vec::new(),
			};
			let after = engine.earliest_expiry(&mut store)?;
			<A::Coord as SealDomain>::rearm_decay(&mut store, settings.fixed_size(), before, after)?;
			rows
		};
		Self::emit_rows(ctx, rows)
	}

	fn apply(&mut self, ctx: &mut impl GuestContext, change: impl ChangeView) -> Result<()> {
		let groups = route_rolling(&self.aggregator, self.pane, ctx, &change)?;
		if groups.is_empty() {
			return Ok(());
		}
		let rows = self.apply_rolling(ctx, groups)?;
		Self::emit_rows(ctx, rows)
	}
}
