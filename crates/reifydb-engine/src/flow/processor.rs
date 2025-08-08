use super::change::{Change, Diff};
use super::flow::Flow;
use super::node::{NodeId, NodeType, OperatorType};
use super::operators::{FilterOperator, MapOperator, Operator, OperatorContext};
use crate::Result;
use crate::columnar::Columns;
use reifydb_catalog::sequence::TableRowSequence;
use reifydb_core::interface::{
    ActiveCommandTransaction, Column, ColumnId, ColumnIndex, EncodableKey, EncodableKeyRange,
    SchemaId, Table, TableId, TableRowKey, TableRowKeyRange, UnversionedTransaction,
    VersionedCommandTransaction, VersionedQueryTransaction, VersionedTransaction,
};
use reifydb_core::row::EncodedRowLayout;
use reifydb_core::{EncodedKeyRange, Type, Value};
use std::collections::Bound::Included;
use std::collections::HashMap;

pub struct FlowProcessor<VT: VersionedTransaction, UT: UnversionedTransaction> {
    flow: Flow,
    operators: HashMap<NodeId, Box<dyn Operator + Send + Sync + 'static> >,
    contexts: HashMap<NodeId, OperatorContext>,
    versioned: VT,
    unversioned: UT,
}

impl<VT: VersionedTransaction, UT: UnversionedTransaction> FlowProcessor<VT, UT> {
    pub fn new(flow: Flow, versioned: VT, unversioned: UT) -> Self {
        Self { flow, operators: HashMap::new(), contexts: HashMap::new(), versioned, unversioned }
    }

    pub fn initialize(&mut self) -> Result<()> {
        // Initialize operators for all nodes
        let node_ids: Vec<NodeId> = self.flow.get_all_nodes().collect();

        for node_id in node_ids {
            if let Some(node) = self.flow.get_node(&node_id) {
                match &node.ty {
                    NodeType::Source { .. } => {
                        // Tables use VersionedStorage directly
                    }
                    NodeType::Operator { operator } => {
                        // Create operator and context
                        let op = self.create_operator(operator)?;
                        self.operators.insert(node_id.clone(), op);
                        self.contexts.insert(node_id.clone(), OperatorContext::new());
                    }
                    NodeType::Sink { .. } => {
                        // Views use VersionedStorage directly
                    }
                }
            }
        }

        Ok(())
    }

    pub fn process_change(&self, node_id: &NodeId, change: Change) -> Result<()> {
        // let mut tx = ;

        let mut txn = ActiveCommandTransaction::new(
            self.versioned.begin_command()?,
            self.unversioned.clone(),
        );

        self.process_change_with_tx(&mut txn, node_id, change)?;
        txn.commit()?;

        Ok(())
    }

    fn process_change_with_tx(
        &self,
        txn: &mut ActiveCommandTransaction<VT, UT>,
        node_id: &NodeId,
        change: Change,
    ) -> Result<()> {
        let (node_type, output_nodes) = if let Some(node) = self.flow.get_node(node_id) {
            (node.ty.clone(), node.outputs.clone())
        } else {
            return Ok(()); // Node not found, nothing to do
        };

        let output_change = match &node_type {
            NodeType::Source { .. } => {
                // Source are handled elsewhere in the system - just propagate
                change
            }
            NodeType::Operator { operator } => {
                // Process through operator
                let transformed_diff = if let (Some(op), Some(context)) =
                    (self.operators.get(node_id), self.contexts.get(node_id))
                {
                    op.apply(context, change)?
                } else {
                    panic!("Operator or context not found");
                };

                // Stateful operators need to persist their internal state
                if operator.is_stateful() {
                    self.apply_diff_to_storage_with_tx(txn, node_id, &transformed_diff)?;
                }

                transformed_diff
            }
            NodeType::Sink { .. } => {
                // Sinks persist the final results
                self.apply_diff_to_storage_with_tx(txn, node_id, &change)?;
                change
            }
        };

        // Propagate to downstream nodes
        for output_id in output_nodes {
            self.process_change_with_tx(txn, &output_id, output_change.clone())?;
        }

        Ok(())
    }

    pub fn hack(
        &self,
        flow: &Flow,
        txn: &mut ActiveCommandTransaction<VT, UT>,
        node_id: &NodeId,
        change: Change,
    ) -> Result<()> {

        let mut operators :  HashMap<NodeId, Box<dyn Operator + Send + Sync + 'static> > = HashMap::new();
        let mut contexts: HashMap<NodeId, OperatorContext> = HashMap::new();

        // Initialize operators for all nodes
        let node_ids: Vec<NodeId> = flow.get_all_nodes().collect();

        for node_id in node_ids {
            if let Some(node) = flow.get_node(&node_id) {
                match &node.ty {
                    NodeType::Source { .. } => {
                        // Tables use VersionedStorage directly
                    }
                    NodeType::Operator { operator } => {
                        // Create operator and context
                        let op = self.create_operator(operator)?;
                        operators.insert(node_id.clone(), op);
                        contexts.insert(node_id.clone(), OperatorContext::new());
                    }
                    NodeType::Sink { .. } => {
                        // Views use VersionedStorage directly
                    }
                }
            }
        }


        let (node_type, output_nodes) = if let Some(node) = flow.get_node(node_id) {
            (node.ty.clone(), node.outputs.clone())
        } else {
            return Ok(()); // Node not found, nothing to do
        };

        let output_change = match &node_type {
            NodeType::Source { .. } => {
                // Source are handled elsewhere in the system - just propagate
                change
            }
            NodeType::Operator { operator } => {
                // Process through operator
                let transformed_diff = if let (Some(op), Some(context)) =
                    (operators.get(node_id), contexts.get(node_id))
                {
                    op.apply(context, change)?
                } else {
                    panic!("Operator or context not found");
                };

                // Stateful operators need to persist their internal state
                if operator.is_stateful() {
                    self.apply_diff_to_storage_with_tx(txn, node_id, &transformed_diff)?;
                }

                transformed_diff
            }
            NodeType::Sink { .. } => {
                // Sinks persist the final results
                self.apply_diff_to_storage_with_tx(txn, node_id, &change)?;
                change
            }
        };

        // Propagate to downstream nodes
        for output_id in output_nodes {
            // self.process_change_with_tx(txn, &output_id, output_change.clone())?;
            self.hack(
                flow,
                txn,
                &output_id,
                output_change.clone(),
            )?;
        }

        Ok(())
    }

    fn create_operator(&self, operator_type: &OperatorType) -> Result<Box<dyn Operator + Send + Sync + 'static>> {
        match operator_type {
            OperatorType::Filter { predicate } => {
                Ok(Box::new(FilterOperator::new(predicate.clone())))
            }
            OperatorType::Map { expressions } => {
                Ok(Box::new(MapOperator::new(expressions.clone())))
            }
            _ => {
                panic!("Operator type {:?} not implemented yet", operator_type)
            }
        }
    }

    fn apply_diff_to_storage_with_tx(
        &self,
        txn: &mut ActiveCommandTransaction<VT, UT>,
        node_id: &NodeId,
        change: &Change,
    ) -> Result<()> {
        let layout = EncodedRowLayout::new(&[Type::Utf8, Type::Int1]);

        let table = Table {
            id: TableId(node_id.0),
            schema: SchemaId(0),
            name: "view".to_string(),
            columns: vec![
                Column {
                    id: ColumnId(0),
                    name: "name".to_string(),
                    ty: Type::Utf8,
                    policies: vec![],
                    index: ColumnIndex(0),
                    auto_increment: false,
                },
                Column {
                    id: ColumnId(1),
                    name: "age".to_string(),
                    ty: Type::Int1,
                    policies: vec![],
                    index: ColumnIndex(1),
                    auto_increment: false,
                },
            ],
        };

        dbg!(&change);

        for diff in &change.diffs {
            match diff {
                Diff::Insert { columns } => {
                    // Convert columns to row deltas
                    // let columns_deltas = self.columns_to_deltas(columns, node_id)?;
                    // deltas.extend(columns_deltas);

                    let row_count = columns.row_count();

                    for row_idx in 0..row_count {
                        // if !mask.get(row_idx) {
                        //     continue;
                        // }

                        let mut row = layout.allocate_row();

                        // For each table column, find if it exists in the input columns
                        for (table_idx, table_column) in table.columns.iter().enumerate() {
                            let value = if let Some(input_column) =
                                columns.iter().find(|col| col.name() == table_column.name)
                            {
                                input_column.data().get_value(row_idx)
                            } else {
                                Value::Undefined
                            };

                            // let policies: Vec<ColumnPolicyKind> =
                            //     table_column.policies.iter().map(|cp| cp.policy.clone()).collect();
                            //
                            // value = coerce_value_to_column_type(
                            //     value,
                            //     table_column.ty,
                            //     ColumnDescriptor::new()
                            //         .with_schema(&schema.name)
                            //         .with_table(&table.name)
                            //         .with_column(&table_column.name)
                            //         .with_column_type(table_column.ty)
                            //         .with_policies(policies),
                            // )?;

                            match value {
                                Value::Bool(v) => layout.set_bool(&mut row, table_idx, v),
                                Value::Float4(v) => layout.set_f32(&mut row, table_idx, *v),
                                Value::Float8(v) => layout.set_f64(&mut row, table_idx, *v),
                                Value::Int1(v) => layout.set_i8(&mut row, table_idx, v),
                                Value::Int2(v) => layout.set_i16(&mut row, table_idx, v),
                                Value::Int4(v) => layout.set_i32(&mut row, table_idx, v),
                                Value::Int8(v) => layout.set_i64(&mut row, table_idx, v),
                                Value::Int16(v) => layout.set_i128(&mut row, table_idx, v),
                                Value::Utf8(v) => layout.set_utf8(&mut row, table_idx, v),
                                Value::Uint1(v) => layout.set_u8(&mut row, table_idx, v),
                                Value::Uint2(v) => layout.set_u16(&mut row, table_idx, v),
                                Value::Uint4(v) => layout.set_u32(&mut row, table_idx, v),
                                Value::Uint8(v) => layout.set_u64(&mut row, table_idx, v),
                                Value::Uint16(v) => layout.set_u128(&mut row, table_idx, v),
                                Value::Date(v) => layout.set_date(&mut row, table_idx, v),
                                Value::DateTime(v) => layout.set_datetime(&mut row, table_idx, v),
                                Value::Time(v) => layout.set_time(&mut row, table_idx, v),
                                Value::Interval(v) => layout.set_interval(&mut row, table_idx, v),
                                Value::RowId(_v) => {}
                                Value::Uuid4(v) => layout.set_uuid4(&mut row, table_idx, v),
                                Value::Uuid7(v) => layout.set_uuid7(&mut row, table_idx, v),
                                Value::Blob(v) => layout.set_blob(&mut row, table_idx, &v),
                                Value::Undefined => layout.set_undefined(&mut row, table_idx),
                            }
                        }

                        // Insert the row into the database
                        let row_id = TableRowSequence::next_row_id(txn, TableId(node_id.0))?;
                        txn.set(
                            &TableRowKey { table: TableId(node_id.0), row: row_id }.encode(),
                            row,
                        )
                        .unwrap();

                        // inserted_count += 1;
                    }
                }
                Diff::Update { old: _, new: _ } => {
                    // For updates, we could implement a more sophisticated approach
                    // For now, just insert the new columns
                    // let columns_deltas = self.columns_to_deltas(new, node_id)?;
                    // deltas.extend(columns_deltas);
                    todo!()
                }
                Diff::Remove { columns: _ } => {
                    // Convert columns to remove deltas
                    // let columns_deltas = self.columns_to_remove_deltas(columns, node_id)?;
                    // deltas.extend(columns_deltas);
                    todo!()
                }
            }
        }

        Ok(())
    }

    pub fn get_view_data(&self, view_name: &str) -> Result<Columns> {
        // Find view node and read from versioned storage
        for node_id in self.flow.get_all_nodes() {
            if let Some(node) = self.flow.get_node(&node_id) {
                if let NodeType::Sink { name, .. } = &node.ty {
                    if name == view_name {
                        return self.read_columns_from_storage(&node_id);
                    }
                }
            }
        }
        panic!("View {} not found", view_name);
    }

    fn read_columns_from_storage(&self, node_id: &NodeId) -> Result<Columns> {
        // Start a read transaction
        let mut rx = self.versioned.begin_query()?;

        let range = TableRowKeyRange { table: TableId(node_id.0) };
        let versioned_data = rx
            .range(EncodedKeyRange::new(
                Included(range.start().unwrap()),
                Included(range.end().unwrap()),
            ))
            .unwrap();

        let layout = EncodedRowLayout::new(&[Type::Utf8, Type::Int1]);

        let table = Table {
            id: TableId(node_id.0),
            schema: SchemaId(0),
            name: "view".to_string(),
            columns: vec![
                Column {
                    id: ColumnId(0),
                    name: "name".to_string(),
                    ty: Type::Utf8,
                    policies: vec![],
                    index: ColumnIndex(0),
                    auto_increment: false,
                },
                Column {
                    id: ColumnId(1),
                    name: "age".to_string(),
                    ty: Type::Int1,
                    policies: vec![],
                    index: ColumnIndex(1),
                    auto_increment: false,
                },
            ],
        };

        let mut columns = Columns::empty_from_table(&table);
        let mut iter = versioned_data.into_iter();
        while let Some(versioned) = iter.next() {
            columns.append_rows(&layout, [versioned.row])?;
        }
        Ok(columns)
    }
}
