// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Trait and value-type contracts for every catalog object kind ReifyDB knows about.
//!
//! Each submodule defines the trait that downstream catalog implementations satisfy and the associated metadata types
//! for one logical kind (namespaces, tables, views, sources, sinks, series, ring buffers, flows, dictionaries, sum
//! types, handlers, procedures, migrations, policies, identities, tokens, and so on), plus the cross-cutting modules
//! for stable identifiers (`id`), key construction (`key`), on-disk layout (`layout`), change records (`change`),
//! configuration (`config`), and column properties (`property`).
//!
//! Invariant: introducing a new catalog object kind is a three-place change. The contract here, a new `KeyKind` byte in
//! `key/kind.rs`, and the typed key in `key/`, must be added together. Skipping any of the three leaves the catalog
//! inconsistent: an object with no on-disk identity, or a key with no contract, or a contract that no key can point at.

pub mod authentication;
pub mod binding;
pub mod change;
pub mod column;
pub mod config;
pub mod dictionary;
pub mod flow;
pub mod handler;
pub mod id;
pub mod identity;
pub mod key;
pub mod layout;
pub mod migration;
pub mod namespace;
pub mod policy;
pub mod procedure;
pub mod property;
pub mod ringbuffer;
pub mod series;
pub mod shape;
pub mod sink;
pub mod source;
pub mod subscription;
pub mod sumtype;
pub mod table;
pub mod task;
pub mod test;
pub mod token;
pub mod view;
pub mod vtable;
