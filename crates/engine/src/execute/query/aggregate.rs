// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use base::expression::AliasExpression;
use crate::execute::Executor;
use transaction::Rx;

impl Executor {
    pub(crate) fn aggregate(&mut self, rx: &impl Rx, group_by: Vec<AliasExpression>, project: Vec<AliasExpression> ) -> crate::Result<()> {
    	

        Ok(())
    }
}
