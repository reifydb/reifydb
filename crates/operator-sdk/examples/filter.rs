//! Example: Filter Operator
//!
//! This operator filters rows based on a field value threshold.

use reifydb_operator_sdk::prelude::*;

/// A filter operator that filters based on a threshold
#[derive(Default)]
struct FilterOperator {
    field_name: String,
    threshold: i64,
}

impl FilterOperator {
    /// Create a new filter operator
    pub fn new(field_name: impl Into<String>, threshold: i64) -> Self {
        Self {
            field_name: field_name.into(),
            threshold,
        }
    }
}

impl Operator for FilterOperator {
    fn initialize(&mut self, config: &[u8]) -> Result<()> {
        // Parse configuration
        if let Ok(config_str) = std::str::from_utf8(config) {
            // Simple config format: "field_name:threshold"
            if let Some((field, threshold_str)) = config_str.split_once(':') {
                self.field_name = field.to_string();
                self.threshold = threshold_str.parse().unwrap_or(0);
            }
        }
        Ok(())
    }

    fn apply(&mut self, _ctx: &mut OperatorContext, input: FlowChange) -> Result<FlowChange> {
        // Filter rows where the field value is greater than threshold
        let filtered = input.filter_rows(|_row| {
            // In a real implementation, we would extract the field value from the row
            // and compare it to the threshold. For this example, we'll just
            // accept all rows.
            true
        });

        Ok(filtered)
    }

    fn metadata(&self) -> OperatorMetadata {
        OperatorMetadata {
            name: "filter",
            version: 1,
            capabilities: Capabilities::new(),
        }
    }
}

// Export the operator for FFI
export_operator!(FilterOperator);

// Main function for testing - in production, compile as cdylib
fn main() {
    println!("Filter operator example");
    println!("This should be compiled as a dynamic library (cdylib) for use with ReifyDB");
    println!("Use: cargo build --example filter --release");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_filter_initialization() {
        let mut operator = FilterOperator::default();
        let config = b"value:100";
        operator.initialize(config).unwrap();

        assert_eq!(operator.field_name, "value");
        assert_eq!(operator.threshold, 100);
    }

    #[test]
    fn test_filter_apply() {
        let operator = FilterOperator::new("value", 50);

        let input = flow_change! {
            diffs: [
                insert: { "id": 1, "value": 100 },
                insert: { "id": 2, "value": 25 },
            ],
            version: 1
        };

        // In this simplified example, all rows pass through
        let expected = input.clone();

        test_operator! {
            operator: operator,
            tests: [
                {
                    input: input,
                    output: expected,
                }
            ]
        }
    }
}