// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::session::{RqlParams, SessionConfig};
use reifydb_core::interface::{Engine, Principal, UnversionedTransaction, VersionedTransaction};
use reifydb_core::result::Frame;

pub struct QuerySession<'a, VT, UT> {
    pub(crate) engine: &'a dyn Engine<VT, UT>,
    pub(crate) principal: Principal,
    pub(crate) config: SessionConfig,
}

impl<'a, VT, UT> QuerySession<'a, VT, UT>
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
        
        // Validate read-only
        validate_read_only(&substituted_rql)?;
        
        // Apply configuration limits
        apply_config_limits(&self.config, &substituted_rql)?;
        
        // Execute query
        self.engine.query_as(&self.principal, &substituted_rql)
    }
}

fn validate_read_only(rql: &str) -> crate::Result<()> {
    let write_keywords = [
        "create", "insert", "update", "delete", "drop", "alter", "truncate"
    ];
    
    let rql_lower = rql.to_lowercase();
    for keyword in &write_keywords {
        if rql_lower.contains(keyword) {
            return Err(crate::Error::from(format!(
                "Read-only session cannot execute write operation containing '{}'", 
                keyword
            )));
        }
    }
    
    Ok(())
}

fn apply_config_limits(config: &SessionConfig, rql: &str) -> crate::Result<()> {
    // Check if full table scan is allowed
    if !config.allow_full_scans && !rql.contains("filter") && !rql.contains("take") {
        return Err(crate::Error::from(
            "Full table scans are not allowed in this session"
        ));
    }
    
    // Additional limit checks would go here
    // (timeout, memory, etc. would be enforced at execution time)
    
    Ok(())
}