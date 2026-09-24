// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashMap;

use reifydb_codec::row::operator::state::decode;
use reifydb_core::{
	key::operator::state::{GroupId, GroupStateKey, IntoGroupStateKey},
	state::timer::StateStore,
	value::column::columns::Columns,
};
use reifydb_flow::{
	operator::state::seal::coord::Coord,
	window::engine::{PublishKey, publish::PublishState},
};
use reifydb_value::value::{Value, datetime::DateTime, row_number::RowNumber};

use crate::{
	error::{Result, SdkError},
	flow::operator::{
		column::{row::Row, sink::in_process::InProcessRowSink},
		context::GuestContext,
		view::in_process::InProcessRowView,
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

pub fn row_to_values<R: Row>(row: &R) -> Result<Vec<Value>> {
	let mut sink = InProcessRowSink::new(R::COLUMNS)?;
	row.encode_into(&mut sink)?;
	let columns = sink.finish(vec![RowNumber(0)], DateTime::default())?;
	Ok(columns.row(0))
}

pub fn values_to_row<R: Row>(values: &[Value]) -> Result<R> {
	let names: Vec<&str> = R::COLUMNS.iter().map(|(name, _)| *name).collect();
	if values.len() != names.len() {
		return Err(SdkError::Other(format!(
			"a published row holds {} values, but the output row has {} columns",
			values.len(),
			names.len()
		)));
	}
	let columns = Columns::from_rows(&names, &[values.to_vec()]);
	R::decode_from(&InProcessRowView::new(&columns, 0))?
		.ok_or_else(|| SdkError::Other("a published row does not decode as the output row".to_string()))
}

pub(super) fn publish_row<A>(
	row_number: RowNumber,
	out: Option<A::Output>,
	state: &mut PublishState,
	watermark: Option<A::Coord>,
	emitted: &mut Emitted<A>,
) -> Result<()>
where
	A: Emit,
	A::Output: Row,
{
	let stored = match state.row.take() {
		Some(values) => Some(values_to_row::<A::Output>(&values)?),
		None => None,
	};
	match (out, stored) {
		(Some(out), None) => {
			state.row = Some(row_to_values(&out)?);
			emitted.0.push((row_number, out));
		}
		(Some(out), Some(pre)) => {
			state.row = Some(row_to_values(&out)?);
			emitted.1.push((row_number, Some(pre), out));
		}
		(None, Some(pre)) => emitted.2.push((row_number, pre)),
		(None, None) => {}
	}
	state.last_publish = watermark.map(|watermark| watermark.to_order());
	state.dirty = false;
	Ok(())
}

pub(super) fn load_publish_states<C: GuestContext>(
	store: &mut GuestAsHost<'_, C>,
	keys: &[PublishKey],
) -> Result<HashMap<GroupId, PublishState>> {
	let by_key: HashMap<GroupStateKey, GroupId> =
		keys.iter().map(|key| (key.into_group_state_key(), key.group)).collect();
	let encoded: Vec<GroupStateKey> = by_key.keys().cloned().collect();
	let mut states: HashMap<GroupId, PublishState> = HashMap::with_capacity(keys.len());
	let mut sizes = HashMap::new();
	store.state_get_many_visit(&encoded, &mut |key, bytes| {
		if let Some(group) = by_key.get(&key) {
			sizes.insert(key, bytes.byte_size());
			states.insert(*group, decode::<PublishState>(&bytes)?);
		}
		Ok(())
	})?;
	for key in &encoded {
		store.state_classify(key, sizes.get(key).copied());
	}
	Ok(states)
}

#[cfg(test)]
mod tests {
	use reifydb_value::value::{Value, datetime::DateTime};

	use super::{row_to_values, values_to_row};
	use crate::row;

	#[derive(Debug, Clone, PartialEq)]
	struct Published {
		group: String,
		count: u64,
		mean: f64,
		start: DateTime,
		best: Option<i64>,
	}

	row!(Published {
		group: String,
		count: u64,
		mean: f64,
		start: DateTime,
		best: Option<i64>
	});

	#[test]
	fn a_row_round_trips_through_its_stored_values() {
		// A published row that does not come back intact makes the next update retract a value downstream never
		// saw.
		for row in [
			Published {
				group: "BTC".to_string(),
				count: 7,
				mean: 1.5,
				start: DateTime::from_epoch_millis(1_700_000_000_123).unwrap(),
				best: Some(-3),
			},
			Published {
				group: String::new(),
				count: 0,
				mean: 0.0,
				start: DateTime::default(),
				best: None,
			},
		] {
			let values = row_to_values(&row).unwrap();
			assert_eq!(values.len(), 5);
			assert_eq!(values[0], Value::Utf8(row.group.clone()));
			assert_eq!(values[1], Value::Uint8(row.count));
			assert_eq!(values_to_row::<Published>(&values).unwrap(), row);
		}
	}

	#[test]
	fn values_of_the_wrong_width_fail_to_decode() {
		// A stored row from another output shape decoded by position would publish columns under the wrong
		// names.
		assert!(values_to_row::<Published>(&[Value::Uint8(1)]).is_err());
	}
}
