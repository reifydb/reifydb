use super::change::{Change, Diff};
use super::flow::FlowGraph;
use super::node::{NodeId, NodeType, OperatorType};
use super::operators::{FilterOperator, MapOperator, Operator, OperatorContext};
use crate::Result;
use reifydb_catalog::sequence::TableRowSequence;
use reifydb_core::frame::Frame;
use reifydb_core::interface::{
    Column, ColumnId, ColumnIndex, EncodableKey, EncodableKeyRange, Rx, SchemaId, Table, TableId,
    TableRowKey, TableRowKeyRange, Transaction, Tx, UnversionedStorage, VersionedStorage,
};
use reifydb_core::row::Layout;
use reifydb_core::{EncodedKeyRange, Type, Value, Version};
use std::collections::Bound::Included;
use std::collections::HashMap;
use std::marker::PhantomData;

pub struct FlowEngine<VS: VersionedStorage, US: UnversionedStorage, T: Transaction<VS, US>> {
    graph: FlowGraph,
    operators: HashMap<NodeId, Box<dyn Operator>>,
    contexts: HashMap<NodeId, OperatorContext>,
    transaction: T,
    current_version: Version,
    _phantom: PhantomData<(VS, US)>,
}

impl<T: Transaction<VS, US>, VS: VersionedStorage, US: UnversionedStorage> FlowEngine<VS, US, T> {
    pub fn new(graph: FlowGraph, transaction: T) -> Self {
        Self {
            graph,
            operators: HashMap::new(),
            contexts: HashMap::new(),
            transaction,
            current_version: 0, // Start at version 0
            _phantom: PhantomData,
        }
    }

    pub fn initialize(&mut self) -> Result<()> {
        // Initialize operators for all nodes
        let node_ids: Vec<NodeId> = self.graph.get_all_nodes().collect();

        for node_id in node_ids {
            if let Some(node) = self.graph.get_node(&node_id) {
                match &node.node_type {
                    NodeType::Table { .. } => {
                        // Tables use VersionedStorage directly
                    }
                    NodeType::Operator { operator } => {
                        // Create operator and context
                        let op = self.create_operator(operator)?;
                        self.operators.insert(node_id.clone(), op);
                        self.contexts.insert(node_id.clone(), OperatorContext::new());
                    }
                    NodeType::View { .. } => {
                        // Views use VersionedStorage directly
                    }
                }
            }
        }

        Ok(())
    }

    pub fn process_change(&mut self, node_id: &NodeId, diff: Diff) -> Result<()> {
        // First get the node type and output nodes to avoid borrowing conflicts

        // FIXME this must be transactional here already

        let (node_type, output_nodes) = if let Some(node) = self.graph.get_node(node_id) {
            (node.node_type.clone(), node.outputs.clone())
        } else {
            return Ok(()); // Node not found, nothing to do
        };

        let output_change = match &node_type {
            NodeType::Table { .. } => {
                // Store in versioned storage with transaction
                self.apply_diff_to_storage(node_id, &diff)?;
                diff
            }
            NodeType::Operator { .. } => {
                // Process through operator
                if let (Some(operator), Some(context)) =
                    (self.operators.get_mut(node_id), self.contexts.get_mut(node_id))
                {
                    operator.apply(context, diff)?
                } else {
                    panic!("Operator or context not found");
                }
            }
            NodeType::View { .. } => {
                // Store in versioned storage with transaction
                self.apply_diff_to_storage(node_id, &diff)?;
                diff
            }
        };

        // Propagate to downstream nodes
        for output_id in output_nodes {
            self.process_change(&output_id, output_change.clone())?;
        }

        Ok(())
    }

    fn create_operator(&self, operator_type: &OperatorType) -> Result<Box<dyn Operator>> {
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

    fn apply_diff_to_storage(&mut self, node_id: &NodeId, diff: &Diff) -> Result<()> {
        // Start a transaction
        let mut tx = self.transaction.begin_tx()?;

        // Increment version for this transaction
        self.current_version += 1;

        let layout = Layout::new(&[Type::Int1]);

        let table = Table {
            id: TableId(node_id.0),
            schema: SchemaId(0),
            name: "view".to_string(),
            columns: vec![Column {
                id: ColumnId(0),
                name: "age".to_string(),
                ty: Type::Int1,
                policies: vec![],
                index: ColumnIndex(0),
            }],
        };

        for change in &diff.changes {
            match change {
                Change::Insert { frame } => {
                    // Convert frame to row deltas
                    // let frame_deltas = self.frame_to_deltas(frame, node_id)?;
                    // deltas.extend(frame_deltas);

                    let row_count = frame.row_count();

                    for row_idx in 0..row_count {
                        // if !mask.get(row_idx) {
                        //     continue;
                        // }

                        let mut row = layout.allocate_row();

                        // For each table column, find if it exists in the input frame
                        for (table_idx, table_column) in table.columns.iter().enumerate() {
                            let value = if let Some(input_column) =
                                frame.columns.iter().find(|col| col.name() == table_column.name)
                            {
                                input_column.values().get(row_idx)
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
                                Value::RowId(v) => {}
                                Value::Uuid4(v) => layout.set_uuid4(&mut row, table_idx, v),
                                Value::Uuid7(v) => layout.set_uuid7(&mut row, table_idx, v),
                                Value::Undefined => layout.set_undefined(&mut row, table_idx),
                            }
                        }

                        // Insert the row into the database
                        let row_id = TableRowSequence::next_row_id(&mut tx, TableId(node_id.0))?;
                        tx.set(
                            &TableRowKey { table: TableId(node_id.0), row: row_id }.encode(),
                            row,
                        )
                        .unwrap();

                        // inserted_count += 1;
                    }
                }
                Change::Update { old: _, new: _ } => {
                    // For updates, we could implement a more sophisticated approach
                    // For now, just insert the new frame
                    // let frame_deltas = self.frame_to_deltas(new, node_id)?;
                    // deltas.extend(frame_deltas);
                    todo!()
                }
                Change::Remove { frame: _ } => {
                    // Convert frame to remove deltas
                    // let frame_deltas = self.frame_to_remove_deltas(frame, node_id)?;
                    // deltas.extend(frame_deltas);
                    todo!()
                }
            }
        }

        // // Apply all deltas to versioned storage in a single transaction
        // if !deltas.is_empty() {
        //     let versioned_storage = self.transaction.versioned();
        //     versioned_storage.apply(CowVec::from(deltas), self.current_version)?;
        // }

        // Commit transaction
        tx.commit()?;

        Ok(())
    }

    pub fn get_view_data(&self, view_name: &str) -> Result<Frame> {
        // Find view node and read from versioned storage
        for node_id in self.graph.get_all_nodes() {
            if let Some(node) = self.graph.get_node(&node_id) {
                if let NodeType::View { name, .. } = &node.node_type {
                    if name == view_name {
                        return self.read_frame_from_storage(&node_id);
                    }
                }
            }
        }
        panic!("View {} not found", view_name);
    }

    fn read_frame_from_storage(&self, node_id: &NodeId) -> Result<Frame> {
        // Start a read transaction
        let mut rx = self.transaction.begin_rx()?;

        let range = TableRowKeyRange { table: TableId(node_id.0) };
        let versioned_data = rx
            .scan_range(EncodedKeyRange::new(
                Included(range.start().unwrap()),
                Included(range.end().unwrap()),
            ))
            .unwrap();

        let layout = Layout::new(&[Type::Int1]);

        let table = Table {
            id: TableId(node_id.0),
            schema: SchemaId(0),
            name: "view".to_string(),
            columns: vec![Column {
                id: ColumnId(0),
                name: "age".to_string(),
                ty: Type::Int1,
                policies: vec![],
                index: ColumnIndex(0),
            }],
        };

        let mut frame = Frame::empty_from_table(&table);
        let mut iter = versioned_data.into_iter();
        while let Some(versioned) = iter.next() {
            frame.append_rows(&layout, [versioned.row])?;
        }

        Ok(frame)
    }

    pub fn get_graph(&self) -> &FlowGraph {
        &self.graph
    }
}
