use crate::flow::change::Diff;
use crate::flow::state::StateStore;

pub trait Operator {
    /// Apply the operator to a change and return the resulting change
    fn apply(&mut self, ctx: &mut OperatorContext, change: Diff) -> crate::Result<Diff>;
}

pub struct OperatorContext {
    pub state: StateStore,
}

impl OperatorContext {
    pub fn new() -> Self {
        Self {
            state: StateStore::new(),
        }
    }
    
    pub fn with_state(state: StateStore) -> Self {
        Self { state }
    }
}