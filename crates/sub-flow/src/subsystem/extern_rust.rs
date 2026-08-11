// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{fs::read_dir, path::PathBuf};

use reifydb_core::event::{
	EventBus,
	operator::{OperatorColumn, OperatorLoadedEvent},
};
use reifydb_value::Result;
use tracing::{info, instrument};

use crate::operator::extern_rust::{ExternRustOperatorColumn, extern_rust_operator_loader};

#[instrument(name = "flow::subsystem::load_extern_rust_operators", level = "info", skip(event_bus), fields(dir = ?dir))]
pub fn load_extern_rust_operators(dir: &PathBuf, event_bus: &EventBus) -> Result<()> {
	let loader = extern_rust_operator_loader();

	let entries = read_dir(dir).unwrap();

	for entry in entries {
		let entry = entry.unwrap();
		let path = entry.path();

		if !path.is_file() {
			continue;
		}

		let is_shared_lib = path.extension().is_some_and(|ext| ext == "so" || ext == "dylib");
		if !is_shared_lib {
			continue;
		}

		let mut guard = loader.write();
		let info = match guard.register_operator(&path)? {
			Some(info) => info,
			None => {
				continue;
			}
		};

		info!("Registered extern-Rust operator: {} from {:?}", info.operator, path);

		fn convert_columns(columns: &[ExternRustOperatorColumn]) -> Vec<OperatorColumn> {
			columns.iter()
				.map(|c| OperatorColumn {
					name: c.name.clone(),
					field_type: c.field_type.clone(),
					description: c.description.clone(),
				})
				.collect()
		}

		let event_bus = event_bus.clone();
		let event = OperatorLoadedEvent::new(
			info.operator,
			info.library_path,
			Some(info.abi_tag),
			info.version,
			info.description,
			convert_columns(&info.input_columns),
			convert_columns(&info.output_columns),
			info.capabilities,
		);

		event_bus.emit(event);
	}

	Ok(())
}
