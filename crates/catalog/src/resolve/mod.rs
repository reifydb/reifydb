// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Catalog entity resolution module
//!
//! This module provides functions to resolve catalog entity IDs to their fully resolved
//! counterparts, including namespace resolution and identifier creation.

mod flow;
mod namespace;
mod primitive;
mod ringbuffer;
mod sequence;
mod table;
mod view;

pub use flow::resolve_flow;
pub use namespace::resolve_namespace;
pub use ringbuffer::resolve_ringbuffer;
pub use table::resolve_table;
pub use view::resolve_view;
