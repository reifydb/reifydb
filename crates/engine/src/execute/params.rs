use reifydb_core::interface::Params;
use reifydb_core::value::Value;

#[derive(Debug, Clone)]
pub struct ParamContext {
    params: Params,
}

impl ParamContext {
    pub fn new(params: Params) -> Self {
        Self { params }
    }
    
    pub fn empty() -> Self {
        Self {
            params: Params::None,
        }
    }
    
    pub fn get_positional(&self, index: usize) -> Option<&Value> {
        self.params.get_positional(index)
    }
    
    pub fn get_named(&self, name: &str) -> Option<&Value> {
        self.params.get_named(name)
    }
}