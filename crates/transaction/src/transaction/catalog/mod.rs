// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use reifydb_core::interface::catalog::change::CatalogTrackChangeOperations;

use crate::transaction::admin::AdminTransaction;

pub mod authentication;
pub mod binding;
pub mod column_snapshot;
pub mod config;
pub mod dictionary;
pub mod flow;
pub mod flow_edge;
pub mod flow_node;
pub mod granted_role;
pub mod handler;
pub mod identity;
pub mod migration;
pub mod namespace;
pub mod operator_settings;
pub mod policy;
pub mod procedure;
pub mod ringbuffer;
pub mod role;
pub mod row_settings;
pub mod series;
pub mod sink;
pub mod source;
pub mod sumtype;
pub mod table;
pub mod test;
pub mod view;

impl CatalogTrackChangeOperations for AdminTransaction {}
