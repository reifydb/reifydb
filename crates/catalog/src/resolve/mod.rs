// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Catalog entity resolution module
//!
//! This module provides functions to resolve catalog entity IDs to their fully resolved
//! counterparts, including namespace resolution and identifier creation.

mod flow;
mod namespace;
mod ring_buffer;
mod sequence;
mod source;
mod table;
mod view;

pub use flow::resolve_flow;
pub use namespace::resolve_namespace;
pub use ring_buffer::resolve_ring_buffer;
pub use table::resolve_table;
pub use view::resolve_view;
