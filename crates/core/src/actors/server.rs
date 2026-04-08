// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Shared response types for network server actors.

use std::{collections::HashMap, time::Duration};

use reifydb_type::{
	error::Diagnostic,
	value::{frame::frame::Frame, identity::IdentityId},
};

use crate::interface::catalog::id::SubscriptionId;

/// Response from an engine dispatch operation (query, command, admin).
pub enum ServerResponse {
	/// Operation succeeded with result frames and compute duration.
	Success {
		frames: Vec<Frame>,
		duration: Duration,
	},
	/// Engine returned an error.
	EngineError {
		diagnostic: Box<Diagnostic>,
		statement: String,
	},
}

/// Response from an authentication attempt.
pub enum ServerAuthResponse {
	/// Authentication succeeded.
	Authenticated {
		identity: IdentityId,
		token: String,
	},
	/// Challenge-response round-trip required.
	Challenge {
		challenge_id: String,
		payload: HashMap<String, String>,
	},
	/// Authentication failed.
	Failed {
		reason: String,
	},
	/// Internal error during authentication.
	Error(String),
}

/// Response from a logout attempt.
pub enum ServerLogoutResponse {
	/// Token successfully revoked.
	Ok,
	/// Token was invalid or already expired.
	InvalidToken,
	/// Internal error during logout.
	Error(String),
}

/// Response from a subscribe operation.
pub enum ServerSubscribeResponse {
	/// Subscription created successfully.
	Subscribed {
		subscription_id: SubscriptionId,
		frames: Vec<Frame>,
		duration: Duration,
	},
	/// Engine returned an error.
	EngineError {
		diagnostic: Box<Diagnostic>,
		statement: String,
	},
}
