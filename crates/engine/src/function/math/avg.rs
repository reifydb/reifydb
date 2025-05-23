// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use base::Value;
use base::expression::Expression;
use base::function::{Function, FunctionError, FunctionExecutor, FunctionMode};

pub struct AvgFunction;

impl Function for AvgFunction {
    fn name(&self) -> &str {
        "avg"
    }
    fn modes(&self) -> &'static [FunctionMode] {
        &[FunctionMode::Scalar, FunctionMode::Aggregate]
    }

    fn prepare(&self, _args: &[Expression]) -> Result<Box<dyn FunctionExecutor>, FunctionError> {
        // Ok(Box::new(AvgExecutor { sum: Value::Float8(OrderedF64(0.0)), count: 0 }))
        todo!()
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

    fn old_eval_scalar(&self, args: &[Value]) -> Result<Value, FunctionError> {
        // let mut sum = Value::Float8(OrderedF64(0.0));
        // let mut count = 0usize;
        // for arg in args {
        //     match arg {
        //         Value::Int2(a) => {
        //             match &mut sum {
        //                 Value::Float8(v) => {
        //                     *v += *a as f64;
        //                 }
        //                 _ => unimplemented!(),
        //             }
        //
        //             count += 1;
        //         }
        //         _ => unimplemented!(),
        //     }
        // }
        //
        // if count == 0 {
        //     Ok(Value::Undefined)
        // } else {
        //     match sum {
        //         Value::Float8(sum) => Ok(Value::Float8(OrderedF64(sum.0 / count as f64))),
        //         _ => unimplemented!(),
        //     }
        // }
        todo!()
    }

    fn eval_aggregate(&mut self, column: &[Value]) -> Result<(), FunctionError> {
        // for value in column {
        //     match value {
        //         Value::Int2(value) => {
        //             match &mut self.sum {
        //                 Value::Float8(v) => {
        //                     v.0 += *value as f64;
        //                 }
        //                 _ => unimplemented!(),
        //             }
        //
        //             self.count += 1;
        //         }
        //         _ => unimplemented!(),
        //     }
        // }
        // Ok(())
        todo!()
    }

    fn finalize_aggregate(&self) -> Result<Value, FunctionError> {
        // if self.count == 0 {
        //     Ok(Value::Undefined)
        // } else {
        //     match self.sum {
        //         Value::Float8(sum) => Ok(Value::Float8(sum / self.count as f64)),
        //         _ => unimplemented!(),
        //     }
        // }
        todo!()
    }
}
