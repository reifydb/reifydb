// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

pub mod changed;
pub mod event;
pub mod handler;

use std::sync::Arc;

use changed::TestingChanged;
use event::TestingEventsDispatched;
use handler::TestingHandlersInvoked;

use crate::routine::registry::RoutinesConfigurator;

pub fn register_testing_native_procedures(builder: RoutinesConfigurator) -> RoutinesConfigurator {
	builder.register_procedure(Arc::new(TestingEventsDispatched::new()))
		.register_procedure(Arc::new(TestingHandlersInvoked::new()))
		.register_procedure(Arc::new(TestingChanged::new("tables")))
		.register_procedure(Arc::new(TestingChanged::new("views")))
		.register_procedure(Arc::new(TestingChanged::new("series")))
		.register_procedure(Arc::new(TestingChanged::new("ringbuffers")))
		.register_procedure(Arc::new(TestingChanged::new("dictionaries")))
}
