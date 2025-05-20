// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::execute::{Executor, evaluate};
use base::expression::Expression;
use base::function::{FunctionError, FunctionMode, FunctionResult};
use base::{Row, Value};
use transaction::StoreRx;

impl Executor {
    /// calls a function within a projection and changes a single value
    pub fn call_projection(&self, func: &str, mut args: Vec<Value>) -> Result<Value, String> {
        if func == "abs" {
            let value = args.pop().unwrap();

            match value {
                Value::Int2(value) => return Ok(Value::Int2(value.abs())),
                _ => unimplemented!(),
            }
        }
        unimplemented!();
    }

    pub fn eval_function<S: StoreRx>(
        &self,
        function_name: &str,
        mode: FunctionMode,
        args: Vec<Expression>,
        row: Option<&Row>,
        store: Option<&S>,
    ) -> Result<FunctionResult, FunctionError> {
        let func = self
            .functions
            .get(function_name)
            .ok_or(FunctionError::UnknownFunction(function_name.to_string()))?;

        let executor = func.prepare(&args)?;
    
        let modes = func.modes();
        if modes.contains(&mode) {
            match mode {
                FunctionMode::Scalar => {
                    let args = self.eval_args(args, row, store);
                    let value = executor.eval_scalar(&args)?;
                    Ok(FunctionResult::Scalar(value))
                }
                FunctionMode::Generator => {
                    let values = self.eval_args(args, row, store);
                    let rows = executor.eval_generator(&values)?;
                    Ok(FunctionResult::Rows(rows))
                }
                FunctionMode::Aggregate => {
                    let input = row.ok_or(FunctionError::MissingInput {
                        function: function_name.to_string(),
                    })?;
                    let mut exec = executor;
                    exec.eval_aggregate(input)?;
                    let result = exec.finalize_aggregate()?;
                    Ok(FunctionResult::Scalar(result))
                }
            }
        } else {
            Err(FunctionError::UnsupportedMode {
                function: function_name.to_string(),
                mode: mode.clone(),
            })
        }
    }

    fn eval_args<S: StoreRx>(
        &self,
        args: Vec<Expression>,
        row: Option<&Row>,
        store: Option<&S>,
    ) -> Vec<Value> {
        args.into_iter().map(|a| evaluate(a, row, store).unwrap()).collect::<Vec<_>>()
    }
}
