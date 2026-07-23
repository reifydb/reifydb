// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! The retention plane: policy and floors above the lane that executes them.
//!
//! The lane in `crate::actor` answers "when does this class get a slice". The plane answers the three questions it
//! cannot: up to which version may this class reclaim ([`ledger`]), how far back must the epoch stay answerable
//! ([`horizon`]), and how far behind is each class right now ([`metrics`]).
//!
//! Splitting floors out of the executors is the point. Six classes each computing their own cutoff privately is how
//! a wrong or wedged floor stayed invisible until disk grew; a single ledger makes every floor inspectable, and
//! makes it structural that a class is constrained by exactly the readers its class declares.

pub mod horizon;
pub mod ledger;
pub mod metrics;
