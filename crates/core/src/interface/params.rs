use crate::value::Value;
use std::collections::HashMap;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum Params {
    #[default]
    None,
    Positional(Vec<Value>),
    Named(HashMap<String, Value>),
}

impl Params {
    pub fn get_positional(&self, index: usize) -> Option<&Value> {
        match self {
            Params::Positional(values) => values.get(index),
            _ => None,
        }
    }

    pub fn get_named(&self, name: &str) -> Option<&Value> {
        match self {
            Params::Named(map) => map.get(name),
            _ => None,
        }
    }
}

impl From<()> for Params {
    fn from(_: ()) -> Self {
        Params::None
    }
}

impl From<Vec<Value>> for Params {
    fn from(values: Vec<Value>) -> Self {
        Params::Positional(values)
    }
}

impl From<HashMap<String, Value>> for Params {
    fn from(map: HashMap<String, Value>) -> Self {
        Params::Named(map)
    }
}

impl<const N: usize> From<[Value; N]> for Params {
    fn from(values: [Value; N]) -> Self {
        Params::Positional(values.to_vec())
    }
}
