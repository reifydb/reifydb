// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

define_event! {
	pub struct ProcedureCreatedEvent {
		pub procedure_name: String,
		pub namespace: String,
	}
}

define_event! {
	pub struct ProcedureExecutedEvent {
		pub procedure_name: String,
		pub duration: std::time::Duration,
	}
}
