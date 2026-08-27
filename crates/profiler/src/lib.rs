// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#![cfg_attr(not(debug_assertions), deny(clippy::disallowed_methods))]
#![cfg_attr(debug_assertions, warn(clippy::disallowed_methods))]
#![cfg_attr(not(debug_assertions), deny(warnings))]
#![allow(clippy::tabs_in_doc_comments)]

pub mod callsite;
pub mod category;
pub mod event;
pub mod format;
pub mod intern;
pub mod layer;
pub mod percentile;
pub mod record;
pub mod scope;
pub mod sink;
pub mod spec;
pub mod summary;
pub mod visit;
