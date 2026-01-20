// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! FFI operator loading for the flow subsystem

use std::{fs::read_dir, path::PathBuf};

use reifydb_core::event::{
	EventBus,
	flow::{FlowOperatorLoadedEvent, OperatorColumnDef},
};
use tracing::{debug, instrument};

use crate::ffi::loader::{ColumnDefInfo, ffi_operator_loader};

/// Load FFI operators from a directory into the global loader.
///
/// This should be called ONCE during subsystem initialization, before any FlowEngine instances are created.
/// All operators in the directory will be loaded and registered, triggering FlowOperatorLoadedEvent for each.
#[instrument(name = "flow::subsystem::load_ffi_operators", level = "debug", skip(event_bus), fields(dir = ?dir))]
pub fn load_ffi_operators(dir: &PathBuf, event_bus: &EventBus) -> reifydb_type::Result<()> {
	let loader = ffi_operator_loader();

	// Scan directory for shared libraries
	let entries = read_dir(dir).unwrap();

	for entry in entries {
		let entry = entry.unwrap();
		let path = entry.path();

		if !path.is_file() {
			continue;
		}

		let is_shared_lib = path.extension().map_or(false, |ext| ext == "so" || ext == "dylib");
		if !is_shared_lib {
			continue;
		}

		// Register the operator without instantiating it
		let mut guard = loader.write().unwrap();
		let info = match guard.register_operator(&path)? {
			Some(info) => info,
			None => {
				// Not a valid FFI operator, skip silently
				continue;
			}
		};

		debug!("Registered FFI operator: {} from {:?}", info.operator, path);

		// Convert column definitions to event format
		fn convert_column_defs(columns: &[ColumnDefInfo]) -> Vec<OperatorColumnDef> {
			columns.iter()
				.map(|c| OperatorColumnDef {
					name: c.name.clone(),
					field_type: c.field_type,
					description: c.description.clone(),
				})
				.collect()
		}

		// Emit event for loaded operator
		let event_bus = event_bus.clone();
		let event = FlowOperatorLoadedEvent::new(
			info.operator,
			info.library_path,
			info.api,
			info.version,
			info.description,
			convert_column_defs(&info.input_columns),
			convert_column_defs(&info.output_columns),
			info.capabilities,
		);

		event_bus.emit(event);
	}

	Ok(())
}
