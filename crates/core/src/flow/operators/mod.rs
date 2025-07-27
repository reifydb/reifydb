pub mod base;
pub mod filter;
pub mod map;

pub use base::{Operator, OperatorContext};
pub use filter::FilterOperator;
pub use map::MapOperator;