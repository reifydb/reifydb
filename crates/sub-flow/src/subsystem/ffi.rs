// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{fs::read_dir, path::PathBuf};

use reifydb_core::event::{
	EventBus,
	flow::{FlowOperatorLoadedEvent, OperatorColumn},
};
use reifydb_extension::operator::ffi_loader::{ColumnInfo, ffi_operator_loader};
use reifydb_type::Result;
use tracing::{debug, instrument};

#[instrument(name = "flow::subsystem::load_ffi_operators", level = "debug", skip(event_bus), fields(dir = ?dir))]
pub fn load_ffi_operators(dir: &PathBuf, event_bus: &EventBus) -> Result<()> {
	let loader = ffi_operator_loader();

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

		let mut guard = loader.write().unwrap();
		let info = match guard.register_operator(&path)? {
			Some(info) => info,
			None => {
				continue;
			}
		};

		debug!("Registered FFI operator: {} from {:?}", info.operator, path);

		fn convert_columns(columns: &[ColumnInfo]) -> Vec<OperatorColumn> {
			columns.iter()
				.map(|c| OperatorColumn {
					name: c.name.clone(),
					field_type: c.field_type.clone(),
					description: c.description.clone(),
				})
				.collect()
		}

		let event_bus = event_bus.clone();
		let event = FlowOperatorLoadedEvent::new(
			info.operator,
			info.library_path,
			info.api,
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
