// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Subscription management for WebSocket push notifications.
//!
//! This module provides the infrastructure for clients to subscribe to
//! real-time data changes via WebSocket connections.

mod handler;
mod poller;
mod registry;

pub(crate) use handler::handle_subscribe;
pub(crate) use poller::SubscriptionPoller;
pub use registry::{PushMessage, SubscriptionRegistry};
