// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

//! Storage-facing implementation of catalog persistence. Each object kind has a sibling module here that owns the
//! `EncodedKey` layout and the (de)serialisation between the typed catalog object and its on-disk bytes. The
//! higher-level catalog operations in `catalog/` go through this layer rather than reaching into a backend
//! directly, so adding a new store backend means re-implementing this trait surface, not the entire catalog.

pub(crate) mod authentication;
pub(crate) mod binding;
pub mod column;
pub(crate) mod column_property;
pub(crate) mod config;
pub(crate) mod dictionary;
pub(crate) mod flow;
pub(crate) mod flow_edge;
pub(crate) mod flow_node;
pub(crate) mod granted_role;
pub(crate) mod handler;
pub(crate) mod identity;
pub(crate) mod migration;
pub(crate) mod namespace;
pub mod operator_settings;
pub(crate) mod policy;
#[allow(dead_code)]
pub(crate) mod primary_key;
pub(crate) mod procedure;
#[allow(dead_code)]
pub(crate) mod retention_strategy;
pub mod ringbuffer;
pub(crate) mod role;
pub mod row_settings;
pub(crate) mod row_shape;
pub(crate) mod sequence;
pub(crate) mod series;
pub(crate) mod shape;
pub(crate) mod sink;
pub(crate) mod source;
pub(crate) mod sumtype;
pub(crate) mod table;
pub(crate) mod token;
pub mod view;
