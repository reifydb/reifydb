// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

mod auth;
mod config;
mod execute;
mod health;
mod metrics;

pub use auth::{handle_auth_status, handle_login, handle_logout};
pub use config::{handle_get_config, handle_update_config};
pub use execute::handle_execute;
pub use health::handle_health;
pub use metrics::handle_metrics;
