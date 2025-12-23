// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

// #![cfg_attr(not(debug_assertions), deny(warnings))]

pub mod http;
pub mod utils;
pub mod websocket;

pub use http::{HttpRequest, HttpResponse, HttpResponseBuilder};
use reifydb_core::interface::version::{ComponentType, HasVersion, SystemVersion};
pub use utils::{find_header_end, parse_headers};
pub use websocket::{WebSocketFrame, WebSocketOpcode, build_ws_frame, build_ws_response, parse_ws_frame};

pub struct NetworkVersion;

impl HasVersion for NetworkVersion {
	fn version(&self) -> SystemVersion {
		SystemVersion {
			name: "network".to_string(),
			version: env!("CARGO_PKG_VERSION").to_string(),
			description: "Network protocol handling module".to_string(),
			r#type: ComponentType::Module,
		}
	}
}
