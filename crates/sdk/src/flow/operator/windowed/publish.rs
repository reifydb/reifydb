// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashMap;

use reifydb_codec::row::operator::state::{StateCodec, decode};
use reifydb_core::{
	key::operator::state::{GroupId, GroupStateKey, IntoGroupStateKey},
	state::timer::StateStore,
};
use reifydb_flow_async::{
	operator::state::seal::coord::Coord,
	window::engine::{PublishKey, publish::PublishState},
};
use reifydb_value::{error::Error as ValueError, value::row_number::RowNumber};

use crate::{
	error::Result,
	flow::operator::{
		context::GuestContext,
		windowed::{
			guest_as_host::GuestAsHost,
			operator::{Emit, WindowedOperator},
		},
	},
};

pub(super) type Rows<A> = Vec<(RowNumber, <A as WindowedOperator>::Output)>;
pub(super) type UpdateRow<A> = (RowNumber, Option<<A as WindowedOperator>::Output>, <A as WindowedOperator>::Output);
pub(super) type UpdateRows<A> = Vec<UpdateRow<A>>;
pub(super) type Emitted<A> = (Rows<A>, UpdateRows<A>, Rows<A>);

pub(super) fn publish_row<A>(
	row_number: RowNumber,
	out: Option<A::Output>,
	state: &mut PublishState<A::Output>,
	watermark: Option<A::Coord>,
	emitted: &mut Emitted<A>,
) -> Result<()>
where
	A: Emit,
	A::Output: Clone,
{
	let stored = state.row.take();
	match (out, stored) {
		(Some(out), None) => {
			state.row = Some(out.clone());
			emitted.0.push((row_number, out));
		}
		(Some(out), Some(pre)) => {
			state.row = Some(out.clone());
			emitted.1.push((row_number, Some(pre), out));
		}
		(None, Some(pre)) => emitted.2.push((row_number, pre)),
		(None, None) => {}
	}
	state.last_publish = watermark.map(|watermark| watermark.to_order());
	state.dirty = false;
	Ok(())
}

pub(super) fn load_publish_states<C: GuestContext, O: StateCodec>(
	store: &mut GuestAsHost<'_, C>,
	keys: &[PublishKey],
) -> Result<HashMap<GroupId, PublishState<O>>> {
	let by_key: HashMap<GroupStateKey, GroupId> =
		keys.iter().map(|key| (key.into_group_state_key(), key.group)).collect();
	let encoded: Vec<GroupStateKey> = by_key.keys().cloned().collect();
	let mut states: HashMap<GroupId, PublishState<O>> = HashMap::with_capacity(keys.len());
	let mut sizes = HashMap::new();
	for (key, bytes) in encoded.iter().zip(store.state_get_many(&encoded)?) {
		let Some(bytes) = bytes else {
			continue;
		};
		if let Some(group) = by_key.get(key) {
			sizes.insert(key.clone(), bytes.byte_size());
			states.insert(*group, decode::<PublishState<O>>(&bytes).map_err(ValueError::from)?);
		}
	}
	for key in &encoded {
		store.state_classify(key, sizes.get(key).copied());
	}
	Ok(states)
}
