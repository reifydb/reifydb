// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

pub mod authentication;
pub mod binding;
pub mod config;
pub mod dictionary;
pub mod flow;
pub mod granted_role;
pub mod identity;
pub mod namespace;
pub mod operator_retention_strategy;
pub mod operator_ttl;
pub mod policy;
pub mod primary_key;
pub mod procedure;
pub mod ringbuffer;
pub mod role;
pub mod row_shape;
pub mod row_ttl;
pub mod shape_retention_strategy;
pub mod sink;
pub mod source;
pub mod sumtype;
pub mod table;
pub mod view;

use authentication::load_authentications;
use binding::load_bindings;
use config::load_configs;
use dictionary::load_dictionaries;
use flow::load_flows;
use granted_role::load_granted_roles;
use identity::load_identities;
use namespace::load_namespaces;
use operator_retention_strategy::load_operator_retention_strategies;
use operator_ttl::load_operator_ttls;
use policy::load_policies;
use primary_key::load_primary_keys;
use procedure::load_procedures;
use reifydb_transaction::transaction::Transaction;
use ringbuffer::load_ringbuffers;
use role::load_roles;
use row_shape::load_row_shapes;
use row_ttl::load_row_ttls;
use shape_retention_strategy::load_shape_retention_strategies;
use sink::load_sinks;
use source::load_sources;
use sumtype::load_sumtypes;
use table::load_tables;
use view::load_views;

use super::CatalogCache;
use crate::Result;

pub struct CatalogCacheLoader;

impl CatalogCacheLoader {
	pub fn load_all(rx: &mut Transaction<'_>, catalog: &CatalogCache) -> Result<()> {
		load_configs(rx, catalog)?;
		load_namespaces(rx, catalog)?;
		load_primary_keys(rx, catalog)?;

		load_row_shapes(rx, catalog)?;

		load_tables(rx, catalog)?;
		load_views(rx, catalog)?;
		load_flows(rx, catalog)?;
		load_ringbuffers(rx, catalog)?;

		load_shape_retention_strategies(rx, catalog)?;
		load_operator_retention_strategies(rx, catalog)?;
		load_row_ttls(rx, catalog)?;
		load_operator_ttls(rx, catalog)?;

		load_dictionaries(rx, catalog)?;
		load_sumtypes(rx, catalog)?;
		load_procedures(rx, catalog)?;

		load_sources(rx, catalog)?;
		load_sinks(rx, catalog)?;

		load_identities(rx, catalog)?;
		load_authentications(rx, catalog)?;
		load_roles(rx, catalog)?;
		load_granted_roles(rx, catalog)?;
		load_policies(rx, catalog)?;

		load_bindings(rx, catalog)?;

		Ok(())
	}
}
