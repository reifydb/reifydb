// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

mod api;
mod static_handler;

pub use api::{
	handle_auth_status, handle_execute, handle_get_config, handle_health,
	handle_login, handle_logout, handle_metrics, handle_update_config,
};
pub use static_handler::{serve_index, serve_static};
