// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::evaluate;
use crate::evaluate::Evaluator;
use base::Value;
use base::Value::Undefined;
use frame::ColumnValues;

impl Evaluator {
    pub(crate) fn constant(
        &mut self,
        value: Value,
        row_count: usize,
    ) -> evaluate::Result<ColumnValues> {
        Ok(match value {
            Value::Bool(v) => ColumnValues::bool(vec![v.clone(); row_count]),
            Value::Float4(v) => unimplemented!(),
            Value::Float8(v) => ColumnValues::float8(vec![v.value(); row_count]),
            Value::Int2(v) => ColumnValues::int2(vec![v.clone(); row_count]),
            Value::Text(v) => ColumnValues::text(vec![v.clone(); row_count]),
            Value::Uint2(v) => unimplemented!(),
            Undefined => ColumnValues::Undefined(row_count),
        })
    }
}

#[cfg(test)]
mod tests {
    #[test]
    #[ignore]
    fn test() {
        todo!()
    }
}
