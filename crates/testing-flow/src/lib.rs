// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#![cfg_attr(not(debug_assertions), deny(warnings))]
#![allow(clippy::tabs_in_doc_comments)]

use reifydb_flow::operator::apply::ApplyOperator;

pub mod generator;
pub mod guest;
pub mod harness;
pub mod state;

pub type GuestHarness = harness::Harness<ApplyOperator>;
