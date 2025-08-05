// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::session::{RqlParams, SessionConfig};
use reifydb_core::interface::{Engine, Principal, UnversionedTransaction, VersionedTransaction};
use reifydb_core::result::Frame;

pub struct CommandSession<'a, VT, UT> {
    pub(crate) engine: &'a dyn Engine<VT, UT>,
    pub(crate) principal: Principal,
    pub(crate) config: SessionConfig,
}

impl<'a, VT, UT> CommandSession<'a, VT, UT>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{
    pub(crate) fn new(engine: &'a dyn Engine<VT, UT>, principal: Principal, config: SessionConfig) -> Self {
        Self {
            engine,
            principal,
            config,
        }
    }
    
    pub fn query(&self, rql: &str, params: impl Into<RqlParams>) -> crate::Result<Vec<Frame>> {
        let params = params.into();
        let substituted_rql = params.substitute(rql)?;
        
        // Apply configuration limits
        apply_config_limits(&self.config, &substituted_rql)?;
        
        // Execute as read
        self.engine.query_as(&self.principal, &substituted_rql)
    }
    
    pub fn command(&self, rql: &str, params: impl Into<RqlParams>) -> crate::Result<Vec<Frame>> {
        let params = params.into();
        let substituted_rql = params.substitute(rql)?;
        
        // Apply configuration limits
        apply_config_limits(&self.config, &substituted_rql)?;
        
        // Execute as write
        self.engine.command_as(&self.principal, &substituted_rql)
    }
}

fn apply_config_limits(config: &SessionConfig, rql: &str) -> crate::Result<()> {
    // Check if full table scan is allowed (for queries)
    if !config.allow_full_scans {
        // Only check for read operations (from without filter)
        let rql_lower = rql.to_lowercase();
        if rql_lower.contains("from") && 
           !rql_lower.contains("filter") && 
           !rql_lower.contains("take") &&
           !rql_lower.contains("insert") &&
           !rql_lower.contains("update") &&
           !rql_lower.contains("delete") {
            return Err(crate::Error::from(
                "Full table scans are not allowed in this session"
            ));
        }
    }
    
    // Additional limit checks would go here
    
    Ok(())
}