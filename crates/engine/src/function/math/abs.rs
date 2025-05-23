// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use base::expression::Expression;
use base::function::{Function, FunctionError, FunctionExecutor, FunctionMode};
use base::{Value, ValueKind};

pub struct AbsFunction;

impl Function for AbsFunction {
    fn name(&self) -> &str {
        "abs"
    }

    fn modes(&self) -> &'static [FunctionMode] {
        &[FunctionMode::Scalar]
    }

    fn prepare(&self, _args: &[Expression]) -> Result<Box<dyn FunctionExecutor>, FunctionError> {
        Ok(Box::new(AbsExecutor))
    }
}

struct AbsExecutor;

impl FunctionExecutor for AbsExecutor {
    fn name(&self) -> &str {
        "abs"
    }

    fn old_eval_scalar(&self, args: &[Value]) -> Result<Value, FunctionError> {
        match args.get(0) {
            Some(Value::Int2(n)) => Ok(Value::Int2(n.abs())),
            Some(value) => Err(FunctionError::InvalidArgumentType {
                function: self.name().to_string(),
                index: 0,
                expected_one_of: vec![ValueKind::Int2],
                actual: value.into(),
            }),
            None => Err(FunctionError::ArityMismatch {
                function: self.name().to_string(),
                expected: 1,
                actual: 0,
            }),
        }
    }
}
