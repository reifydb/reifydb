// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::execute::Executor;
use crate::frame::aggregate::Aggregate;
use reifydb_rql::expression::{AliasExpression, ColumnExpression, Expression};
use reifydb_storage::{Storage, UnversionedStorage, VersionedStorage};

impl<VS: VersionedStorage, US: UnversionedStorage> Executor<VS, US> {
    pub(crate) fn aggregate(
        &mut self,
        group_by: &[AliasExpression],
        project: &[AliasExpression],
    ) -> crate::Result<()> {
        let mut keys = vec![];
        let mut aggregates = vec![];

        for gb in group_by {
            match &gb.expression {
                Expression::Column(ColumnExpression(c)) => keys.push(c.fragment.as_str()),
                _ => unimplemented!(),
            }
        }

        for p in project {
            match &p.expression {
                Expression::Call(call) => {
                    let func = call.func.0.fragment.as_str();

                    match call.args.first().unwrap() {
                        Expression::Column(ColumnExpression(c)) => match func {
                            "avg" => aggregates.push(Aggregate::Avg(c.fragment.to_string())),
                            "sum" => aggregates.push(Aggregate::Sum(c.fragment.to_string())),
                            "count" => aggregates.push(Aggregate::Count(c.fragment.to_string())),
                            _ => unimplemented!(),
                        },
                        _ => unimplemented!(),
                    }
                }
                expr => {}
            }
        }

        self.frame.aggregate(&keys, &aggregates)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    #[test]
    #[ignore]
    fn implement() {
        todo!()
    }
}
