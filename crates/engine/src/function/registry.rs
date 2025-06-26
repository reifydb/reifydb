// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::function::{AggregateFunction, ScalarFunction};
use std::collections::HashMap;

pub struct Functions {
    scalars: HashMap<String, Box<dyn Fn() -> Box<dyn ScalarFunction>>>,
    aggregates: HashMap<String, Box<dyn Fn() -> Box<dyn AggregateFunction>>>,
}

impl Functions {
    pub fn new() -> Self {
        Self { scalars: HashMap::new(), aggregates: HashMap::new() }
    }

    pub fn get_aggregate(&self, name: &str) -> Option<Box<dyn AggregateFunction>> {
        self.aggregates.get(name).map(|func| func())
    }

    pub fn register_aggregate<F, A>(&mut self, name: &str, init: F)
    where
        F: Fn() -> A + 'static,
        A: AggregateFunction + 'static,
    {
        self.aggregates.insert(
            name.to_string(),
            Box::new(move || Box::new(init()) as Box<dyn AggregateFunction>),
        );
    }

    pub fn get_scalar(&self, name: &str) -> Option<Box<dyn ScalarFunction>> {
        self.scalars.get(name).map(|func| func())
    }

    pub fn register_scalar<F, A>(&mut self, name: &str, init: F)
    where
        F: Fn() -> A + 'static,
        A: ScalarFunction + 'static,
    {
        self.scalars.insert(
            name.to_string(),
            Box::new(move || Box::new(init()) as Box<dyn ScalarFunction>),
        );
    }
}
