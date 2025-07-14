// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::evaluate::{EvaluationContext, Evaluator};
use crate::frame::FrameColumn;
use reifydb_rql::expression::AliasExpression;

impl Evaluator {
    pub(crate) fn alias(
		&mut self,
		expr: &AliasExpression,
		ctx: &EvaluationContext,
    ) -> crate::evaluate::Result<FrameColumn> {
        let evaluated = self.evaluate(&expr.expression, ctx)?;
        Ok(FrameColumn { name: expr.alias.0.fragment.clone(), values: evaluated.values })
    }
}
