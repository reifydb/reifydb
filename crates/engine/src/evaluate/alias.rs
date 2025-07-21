// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::evaluate::{EvaluationContext, Evaluator};
use crate::frame::FrameColumn;
use reifydb_core::value::row_id::ROW_ID_COLUMN_NAME;
use reifydb_rql::expression::AliasExpression;

impl Evaluator {
    pub(crate) fn alias(
		&mut self,
		expr: &AliasExpression,
		ctx: &EvaluationContext,
    ) -> crate::evaluate::Result<FrameColumn> {
        let evaluated = self.evaluate(&expr.expression, ctx)?;
        let alias_name = &expr.alias.0.fragment;
        if alias_name == ROW_ID_COLUMN_NAME {
            panic!("Column name '{}' is reserved for RowId columns", ROW_ID_COLUMN_NAME);
        }
        Ok(FrameColumn { name: alias_name.clone(), values: evaluated.values })
    }
}
