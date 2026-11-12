// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::{datetime::DateTime, row_number::RowNumber};

use crate::flow::operator::windowed::operator::WindowedOperator;

pub(super) type Rows<A> = Vec<(RowNumber, DateTime, <A as WindowedOperator>::Output)>;
pub(super) type UpdateRow<A> =
	(RowNumber, DateTime, DateTime, Option<<A as WindowedOperator>::Output>, <A as WindowedOperator>::Output);
pub(super) type UpdateRows<A> = Vec<UpdateRow<A>>;
pub(super) type Emitted<A> = (Rows<A>, UpdateRows<A>, Rows<A>);
