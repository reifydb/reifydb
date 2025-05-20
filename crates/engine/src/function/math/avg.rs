// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use base::Value;
use base::expression::Expression;
use base::function::{Function, FunctionError, FunctionExecutor, FunctionMode};

pub(crate) struct AvgFunction;

impl Function for AvgFunction {
    fn name(&self) -> &str {
        "avg"
    }
    fn modes(&self) -> &'static [FunctionMode] {
        &[FunctionMode::Scalar, FunctionMode::Aggregate]
    }

    fn prepare(&self, _args: &[Expression]) -> Result<Box<dyn FunctionExecutor>, FunctionError> {
        Ok(Box::new(AvgExecutor { sum: Value::Float8(0.0), count: 0 }))
    }
}

struct AvgExecutor {
    sum: Value,
    count: usize,
}

impl FunctionExecutor for AvgExecutor {
    fn name(&self) -> &str {
        "avg"
    }

    fn eval_scalar(&self, args: &[Value]) -> Result<Value, FunctionError> {
        let mut sum = Value::Float8(0.0);
        let mut count = 0usize;
        for arg in args {
            match arg {
                Value::Int2(a) => {
                    match &mut sum {
                        Value::Float8(v) => {
                            *v += *a as f64;
                        }
                        _ => unimplemented!(),
                    }

                    count += 1;
                }
                _ => unimplemented!(),
            }
        }

        if count == 0 {
            Ok(Value::Undefined)
        } else {
            match sum {
                Value::Float8(sum) => Ok(Value::Float8(sum / count as f64)),
                _ => unimplemented!(),
            }
        }
    }

    fn eval_aggregate(&mut self, row: &[Value]) -> Result<(), FunctionError> {
        if let Some(Value::Float8(f)) = row.get(0) {
            match &mut self.sum {
                Value::Float8(v) => {
                    *v += *f;
                }
                _ => unimplemented!(),
            }

            self.count += 1;
        }
        Ok(())
    }

    fn finalize_aggregate(&self) -> Result<Value, FunctionError> {
        if self.count == 0 {
            Ok(Value::Undefined)
        } else {
            match self.sum {
                Value::Float8(sum) => Ok(Value::Float8(sum / self.count as f64)),
                _ => unimplemented!(),
            }
        }
    }
}
