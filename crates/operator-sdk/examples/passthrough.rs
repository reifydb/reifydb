//! Example: Passthrough Operator
//!
//! This is the simplest possible operator - it passes through
//! all input changes without modification.

use reifydb_operator_sdk::prelude::*;

/// A simple passthrough operator
#[derive(Default)]
struct PassthroughOperator;

impl Operator for PassthroughOperator {
    fn apply(&mut self, _ctx: &mut OperatorContext, input: FlowChange) -> Result<FlowChange> {
        // Simply return the input unchanged
        Ok(input)
    }

    fn metadata(&self) -> OperatorMetadata {
        OperatorMetadata {
            name: "passthrough",
            version: 1,
            capabilities: Capabilities::new(),
        }
    }
}

// Export the operator for FFI
export_operator!(PassthroughOperator);

// Main function for testing - in production, compile as cdylib
fn main() {
    println!("Passthrough operator example");
    println!("This should be compiled as a dynamic library (cdylib) for use with ReifyDB");
    println!("Use: cargo build --example passthrough --release");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_passthrough() {
        let input = flow_change! {
            insert: { "id": 1, "value": "test" },
            version: 1
        };

        let expected = input.clone();

        test_operator! {
            operator: PassthroughOperator::default(),
            tests: [
                {
                    input: input,
                    output: expected,
                }
            ]
        }
    }
}