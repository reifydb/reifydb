// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::Catalog;
use crate::column::ColumnId;
use crate::column_policy::layout::column_policy;
use crate::column_policy::{ColumnPolicy, ColumnPolicyId, ColumnPolicyKind};
use reifydb_core::interface::ColumnPolicyKey;
use reifydb_core::interface::Rx;

impl Catalog {
    pub fn list_column_policies(
        rx: &mut impl Rx,
        column: ColumnId,
    ) -> crate::Result<Vec<ColumnPolicy>> {
        Ok(rx
            .scan_range(ColumnPolicyKey::full_scan(column))?
            .map(|versioned| {
                let row = versioned.row;
                let id = ColumnPolicyId(column_policy::LAYOUT.get_u64(&row, column_policy::ID));
                let column = ColumnId(column_policy::LAYOUT.get_u64(&row, column_policy::COLUMN));

                let policy = ColumnPolicyKind::from_u8(
                    column_policy::LAYOUT.get_u8(&row, column_policy::POLICY),
                    column_policy::LAYOUT.get_u8(&row, column_policy::VALUE),
                );

                ColumnPolicy { id, column, policy }
            })
            .collect::<Vec<_>>())
    }
}

#[cfg(test)]
mod tests {
    use crate::Catalog;
    use crate::column::{ColumnId, ColumnIndex, ColumnToCreate};
    use crate::column_policy::{ColumnPolicyKind, ColumnSaturationPolicy};
    use crate::table::TableId;
    use crate::test_utils::ensure_test_table;
    use reifydb_core::Type;
    use reifydb_transaction::test_utils::TestTransaction;

    #[test]
    fn test_ok() {
        let mut tx = TestTransaction::new();
        ensure_test_table(&mut tx);

        Catalog::create_column(
            &mut tx,
            TableId(1),
            ColumnToCreate {
                span: None,
                schema_name: "test_schema",
                table: TableId(1),
                table_name: "test_table",
                column: "with_policy".to_string(),
                value: Type::Int2,
                if_not_exists: false,
                policies: vec![ColumnPolicyKind::Saturation(ColumnSaturationPolicy::Undefined)],
                index: ColumnIndex(0),
            },
        )
        .unwrap();

        let column = Catalog::get_column(&mut tx, ColumnId(1)).unwrap().unwrap();

        let policies = Catalog::list_column_policies(&mut tx, column.id).unwrap();
        assert_eq!(policies.len(), 1);
        assert_eq!(policies[0].column, column.id);
        assert!(matches!(
            policies[0].policy,
            ColumnPolicyKind::Saturation(ColumnSaturationPolicy::Undefined)
        ));
    }
}
