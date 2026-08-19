// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::key::encoded::EncodedKey;
use reifydb_core::{interface::catalog::flow::OperatorId, state::store::TimerKind};
use reifydb_value::Result;

use crate::{
	timer::{Timer, TimerDue, wheel::TimerWheel},
	transaction::FlowTransaction,
};

pub trait TimerExtension: FlowTransaction {
	fn arm_timer(&mut self, operator: OperatorId, timer: &Timer) -> Result<()> {
		TimerWheel::arm(operator, self, timer)?;
		self.push_armed(TimerDue {
			operator_id: operator,
			due: timer.due,
		});
		Ok(())
	}

	fn disarm_timer(&mut self, operator: OperatorId, timer: &Timer) -> Result<()> {
		TimerWheel::disarm(operator, self, timer)
	}

	fn disarm_timer_by_key(&mut self, operator: OperatorId, kind: TimerKind, key: &EncodedKey) -> Result<()> {
		TimerWheel::disarm_by_key(operator, self, kind, key)
	}
}

impl<T: FlowTransaction> TimerExtension for T {}
