// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::execute::Executor;
use base::Value;

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
}
