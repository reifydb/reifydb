// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

mod frame;
mod handshake;

pub use frame::{
	WebSocketFrame, WebSocketOpcode, build_ws_frame, parse_ws_frame,
};
pub use handshake::build_ws_response;
