// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{HashMap, HashSet},
	fmt::Debug,
	hash::Hash,
	slice::from_ref,
};

use reifydb_codec::{key::encoded::EncodedKey, row::operator::state::StateCodec};
use reifydb_core::{
	internal_err,
	key::operator::state::{GroupId, GroupStateKey, IntoGroupStateKey},
	state::timer::StateStore,
};
use reifydb_value::{Result, util::hash::Hash128};

use crate::{
	operator::{
		state::seal::coord::Coord,
		state_access::{get_classified, set},
	},
	window::{
		accumulator::WindowAccumulator,
		driver::{
			hooks::{WindowEmits, WindowHooks},
			publish::publish,
			route::Routed,
		},
		engine::{EmitKind, ExpiryAnchor, WindowReindex, tumbling::TumblingEngine},
		meta::{EngineMeta, EngineMetaKey},
		prefetch::Prefetched,
		span::{WindowAnchor, WindowSpan},
	},
};

type Contributions<H> = <<H as WindowHooks>::Accumulator as WindowAccumulator>::Contributions;

struct Reindexed<G, C> {
	group: G,
	window_start: C,
	id: GroupId,
	slot_key: EncodedKey,
	prior: Option<u64>,
	new: Option<u64>,
}

fn prior_meta(store: &mut dyn StateStore, group: GroupId) -> Result<Option<EngineMeta>> {
	Ok(get_classified::<_, EngineMeta>(store, from_ref(&EngineMetaKey(group)))?.remove(0))
}

pub fn finish_tumbling<H>(
	engine: &mut TumblingEngine<H::Group, H::Coord, H::Accumulator>,
	store: &mut dyn StateStore,
	routed: Routed<H::Group, H::Coord, Contributions<H>>,
	hooks: &mut H,
	anchor: ExpiryAnchor,
) -> Result<H::Out>
where
	H: WindowHooks,
	H::Group: Clone + Eq + Ord + Hash + Debug + StateCodec,
	H::Coord: WindowAnchor + Hash,
{
	let Routed {
		buckets,
		arrival,
		window_max_coord,
	} = routed;
	let mut identities: HashMap<H::Group, (Hash128, EncodedKey)> = HashMap::new();
	for (group, _) in &arrival {
		if !identities.contains_key(group) {
			identities.insert(group.clone(), hooks.group_identity(group));
		}
	}
	let id_of = |group: &H::Group, start: H::Coord| -> (GroupId, &EncodedKey) {
		let (hash, key) = &identities[group];
		(GroupId::window(*hash, start.to_order()), key)
	};

	let results = engine.apply(
		store,
		buckets,
		&arrival,
		|group, start| {
			let (id, key) = id_of(group, start);
			(id, key.clone())
		},
		|| hooks.new_accumulator(),
	)?;

	let keys: Vec<GroupStateKey> = arrival
		.iter()
		.map(|(group, span)| (&EngineMetaKey(id_of(group, span.start).0)).into_group_state_key())
		.collect();
	let mut prefetched = Prefetched::read(store, keys)?;
	let mut reindexed = Vec::with_capacity(arrival.len());
	let mut reindex = |group: &H::Group, span: WindowSpan<H::Coord>, kind: Option<EmitKind>| -> Result<()> {
		let (id, key) = id_of(group, span.start);
		let window_start = span.start.to_order();
		let prior = prior_meta(&mut prefetched, id)?;
		let prior_last = prior.as_ref().map(|meta| meta.last_event_time);
		let prior_index = prior.is_some().then(|| anchor.of(window_start, prior_last)).flatten();
		if kind == Some(EmitKind::Remove) {
			return Ok(());
		}
		let batch_max = window_max_coord.get(&(group.clone(), span)).map(|coord| coord.to_order());
		let last_event_time = prior_last.max(batch_max);
		reindexed.push(Reindexed {
			group: group.clone(),
			window_start: span.start,
			id,
			slot_key: key.clone(),
			prior: prior_index,
			new: anchor.of(window_start, last_event_time),
		});
		let Some(last_event_time) = last_event_time else {
			return internal_err!(
				"window {window_start} of group {group:?} has neither a stored nor a routed event time"
			);
		};
		set(
			&mut prefetched,
			vec![(
				EngineMetaKey(id),
				EngineMeta {
					last_event_time,
				},
			)],
		)
	};
	for r in &results {
		reindex(&r.group, r.span, Some(r.kind))?;
	}
	let published: HashSet<(H::Group, WindowSpan<H::Coord>)> =
		results.iter().map(|r| (r.group.clone(), r.span)).collect();
	for (group, span) in arrival.iter().filter(|window| !published.contains(window)) {
		reindex(group, *span, None)?;
	}
	drop(reindex);
	let entries: Vec<WindowReindex<'_, H::Group, H::Coord>> = reindexed
		.iter()
		.map(|entry| WindowReindex {
			group: &entry.group,
			window_start: entry.window_start,
			id: entry.id,
			slot_key: &entry.slot_key,
			prior: entry.prior,
			new: entry.new,
		})
		.collect();
	engine.reindex_windows(&mut prefetched, &entries)?;
	prefetched.write()?;

	let mut emits = WindowEmits::default();
	for r in results {
		let Some(time) = r.first_time else {
			return internal_err!(
				"a tumbling result for row {:?} of group {:?} carries no first time",
				r.row_number,
				r.group
			);
		};
		let pre = r.prior.as_ref().and_then(|prior| hooks.build(&r.group, r.span, prior));
		let post = match r.kind {
			EmitKind::Remove => None,
			EmitKind::Insert | EmitKind::Update => hooks.build(&r.group, r.span, &r.value),
		};
		publish(&mut emits, r.row_number, time, time, pre, post);
	}
	hooks.emit(emits)
}
