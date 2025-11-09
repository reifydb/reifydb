//! Declarative macros for operator development

/// Export an operator type for FFI
///
/// This macro generates the required FFI export functions for an operator.
///
/// # Example
/// ```
/// use reifydb_operator_sdk::prelude::*;
///
/// struct MyOperator;
/// impl Operator for MyOperator {
///     // implementation
/// }
///
/// export_operator!(MyOperator);
/// ```
#[macro_export]
macro_rules! export_operator {
    ($operator_type:ty) => {
        // Generate the FFI exports
        #[no_mangle]
        pub extern "C" fn ffi_operator_get_descriptor() -> *const $crate::ffi::FFIOperatorDescriptor {
            static DESCRIPTOR: ::std::sync::OnceLock<$crate::ffi::FFIOperatorDescriptor> =
                ::std::sync::OnceLock::new();

            DESCRIPTOR.get_or_init(|| {
                $crate::ffi::create_descriptor::<$operator_type>()
            })
        }

        #[no_mangle]
        pub extern "C" fn ffi_operator_create(
            config: *const u8,
            config_len: usize,
        ) -> *mut ::std::ffi::c_void {
            $crate::ffi::create_operator_instance::<$operator_type>(config, config_len)
        }
    };
}

/// Define operator metadata
///
/// This macro helps define operator metadata in a declarative way.
///
/// # Example
/// ```
/// operator! {
///     name: "filter",
///     version: 1,
///     capabilities: [stateful, keyed],
///     type: FilterOperator
/// }
/// ```
#[macro_export]
macro_rules! operator {
    (
        name: $name:literal,
        version: $version:literal,
        capabilities: [$($cap:ident),* $(,)?],
        type: $operator_type:ty
    ) => {
        impl $crate::operator::Operator for $operator_type {
            fn metadata(&self) -> $crate::operator::OperatorMetadata {
                $crate::operator::OperatorMetadata {
                    name: $name,
                    version: $version,
                    capabilities: $crate::operator::Capabilities::new()
                        $(.$crate::__capability_method!($cap)(true))*,
                }
            }

            // User must implement apply() and optionally other methods
        }
    };
}

/// Internal macro for capability method names
#[doc(hidden)]
#[macro_export]
macro_rules! __capability_method {
    (stateful) => { with_stateful };
    (keyed) => { with_keyed };
    (windowed) => { with_windowed };
    (batch) => { with_batch };
}

/// Create a flow change from a JSON value
///
/// # Example
/// ```
/// let change = flow_change! {
///     insert: { "id": 1, "name": "Alice" },
///     version: 1
/// };
/// ```
#[macro_export]
macro_rules! flow_change {
    // Insert only
    (insert: $json:tt $(, version: $version:expr)?) => {{
        let row = $crate::builders::RowBuilder::from_json(0u64, serde_json::json!($json));
        $crate::builders::FlowChangeBuilder::new()
            .insert(row)
            $(.with_version($version))?
            .build()
    }};

    // Update only
    (update: { pre: $pre:tt, post: $post:tt } $(, version: $version:expr)?) => {{
        let pre_row = $crate::builders::RowBuilder::from_json(0u64, serde_json::json!($pre));
        let post_row = $crate::builders::RowBuilder::from_json(0u64, serde_json::json!($post));
        $crate::builders::FlowChangeBuilder::new()
            .update(pre_row, post_row)
            $(.with_version($version))?
            .build()
    }};

    // Remove only
    (remove: $json:tt $(, version: $version:expr)?) => {{
        let row = $crate::builders::RowBuilder::from_json(0u64, serde_json::json!($json));
        $crate::builders::FlowChangeBuilder::new()
            .remove(row)
            $(.with_version($version))?
            .build()
    }};

    // Multiple operations
    (diffs: [$($diff:tt),* $(,)?] $(, version: $version:expr)?) => {{
        let mut builder = $crate::builders::FlowChangeBuilder::new();
        $(
            builder = flow_change!(@diff builder, $diff);
        )*
        builder $(.with_version($version))? .build()
    }};

    // Internal: process a single diff
    (@diff $builder:expr, insert: $json:tt) => {{
        let row = $crate::builders::RowBuilder::from_json(0u64, serde_json::json!($json));
        $builder.insert(row)
    }};

    (@diff $builder:expr, update: { pre: $pre:tt, post: $post:tt }) => {{
        let pre_row = $crate::builders::RowBuilder::from_json(0u64, serde_json::json!($pre));
        let post_row = $crate::builders::RowBuilder::from_json(0u64, serde_json::json!($post));
        $builder.update(pre_row, post_row)
    }};

    (@diff $builder:expr, remove: $json:tt) => {{
        let row = $crate::builders::RowBuilder::from_json(0u64, serde_json::json!($json));
        $builder.remove(row)
    }};
}

/// Create a row from JSON
///
/// # Example
/// ```
/// let row = row! {
///     number: 123,
///     data: { "id": 1, "name": "Alice", "age": 30 }
/// };
/// ```
#[macro_export]
macro_rules! row {
    (number: $number:expr, data: $json:tt) => {{
        $crate::builders::RowBuilder::from_json($number, serde_json::json!($json))
    }};

    // Default row number to 0
    ($json:tt) => {{
        $crate::builders::RowBuilder::from_json(0u64, serde_json::json!($json))
    }};
}

/// Assert flow changes are equal (for testing)
///
/// # Example
/// ```
/// assert_flow_change_eq!(actual, expected);
/// ```
#[macro_export]
macro_rules! assert_flow_change_eq {
    ($actual:expr, $expected:expr) => {{
        let actual = &$actual;
        let expected = &$expected;
        assert_eq!(
            actual.version, expected.version,
            "Flow change versions don't match: {} != {}",
            actual.version, expected.version
        );
        assert_eq!(
            actual.diffs.len(), expected.diffs.len(),
            "Flow change diff counts don't match: {} != {}",
            actual.diffs.len(), expected.diffs.len()
        );
        for (i, (actual_diff, expected_diff)) in actual.diffs.iter().zip(expected.diffs.iter()).enumerate() {
            assert_eq!(
                actual_diff, expected_diff,
                "Diff {} doesn't match", i
            );
        }
    }};
}

/// Test an operator with input/output pairs
///
/// # Example
/// ```
/// test_operator! {
///     operator: MyOperator::new(),
///     tests: [
///         {
///             input: flow_change! { insert: { "value": 1 } },
///             output: flow_change! { insert: { "value": 2 } },
///         },
///     ]
/// }
/// ```
#[macro_export]
macro_rules! test_operator {
    (
        operator: $op:expr,
        tests: [
            $(
                {
                    input: $input:expr,
                    output: $output:expr $(,)?
                }
            ),* $(,)?
        ]
    ) => {{
        let mut operator = $op;
        let mut ctx = $crate::context::MockContext::new();

        $(
            let input = $input;
            let expected = $output;
            let actual = operator.apply(ctx.as_mut(), input)
                .expect("Operator apply failed");
            $crate::assert_flow_change_eq!(actual, expected);
        )*
    }};
}