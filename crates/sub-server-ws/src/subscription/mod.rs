// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Subscription management for WebSocket push notifications.
//!
//! This module provides the infrastructure for clients to subscribe to
//! real-time data changes via WebSocket connections.

pub mod handler;
pub mod poller;
pub mod registry;

use poller::SubscriptionPoller;
use registry::{PushMessage, SubscriptionRegistry};
