// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

pub mod changed;
pub mod event;
pub mod handler;

use changed::TestingChanged;
use event::TestingEventsDispatched;
use handler::TestingHandlersInvoked;

use crate::procedure::registry::ProceduresConfigurator;

pub fn register_testing_procedures(builder: ProceduresConfigurator) -> ProceduresConfigurator {
	builder.with_procedure("testing::events::dispatched", TestingEventsDispatched::new)
		.with_procedure("testing::handlers::invoked", TestingHandlersInvoked::new)
		.with_procedure("testing::tables::changed", || TestingChanged::new("tables"))
		.with_procedure("testing::views::changed", || TestingChanged::new("views"))
		.with_procedure("testing::series::changed", || TestingChanged::new("series"))
		.with_procedure("testing::ringbuffers::changed", || TestingChanged::new("ringbuffers"))
		.with_procedure("testing::dictionaries::changed", || TestingChanged::new("dictionaries"))
}
