// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_flow_async::{operator::state::seal::coord::Coord, window::engine::publish::PublishState};
use reifydb_value::value::row_number::RowNumber;

use crate::{
	error::Result,
	flow::operator::windowed::operator::{Emit, WindowedOperator},
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
