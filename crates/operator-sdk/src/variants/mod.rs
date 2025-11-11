//! Common operator variants

mod stateless;
mod stateful;
mod keyed;

pub use stateless::{StatelessOperator, StatelessAdapter, stateless};
pub use stateful::{StatefulPattern, stateful_operator};
pub use keyed::{KeyedOperator, KeyedAdapter, keyed};