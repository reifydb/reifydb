// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::evaluate::{Context, evaluate};
use crate::execute::Executor;
use crate::frame::Column;
use reifydb_rql::expression::Expression;
use reifydb_storage::{UnversionedStorage, VersionedStorage};

impl<VS: VersionedStorage, US: UnversionedStorage> Executor<VS, US> {
    pub(crate) fn filter(&mut self, expression: Expression) -> crate::Result<()> {
        let row_count = self.frame.columns.first().map_or(0, |col| col.data.len());
        let columns: Vec<&Column> = self.frame.columns.iter().map(|c| c).collect();

        // let evaluated_column =
        //     evaluate(&expression, &Context { column: None }, &columns, row_count)?;
        // 
        // dbg!(&evaluated_column);

        Ok(())
    }
}
