use std::collections::{Bound::Included, HashMap};

use reifydb_catalog::sequence::ViewRowSequence;
use reifydb_core::{
	EncodedKeyRange, Type, Value,
	interface::{
		ActiveCommandTransaction, ColumnIndex, EncodableKey,
		EncodableKeyRange, Evaluate, SchemaId, TableColumnDef,
		TableColumnId, TableDef, TableId, Transaction,
		VersionedCommandTransaction, VersionedQueryTransaction,
		VersionedTransaction, ViewColumnDef, ViewColumnId, ViewDef,
		ViewId, ViewRowKey, ViewRowKeyRange,
	},
	row::EncodedRowLayout,
	value::columnar::Columns,
};

use crate::{
	core::{Change, Diff, Flow, NodeId, NodeType, OperatorType},
	operator::{FilterOperator, MapOperator, Operator, OperatorContext},
};

pub struct LegacyFlowProcessor<T: Transaction, E: Evaluate> {
	flow: Flow,
	operators:
		HashMap<NodeId, Box<dyn Operator<E> + Send + Sync + 'static>>,
	versioned: T::Versioned,
	unversioned: T::Unversioned,
	cdc: T::Cdc,
	evaluator: E,
}

impl<T: Transaction, E: Evaluate> LegacyFlowProcessor<T, E> {
	pub fn new(
		flow: Flow,
		versioned: T::Versioned,
		unversioned: T::Unversioned,
		cdc: T::Cdc,
		evaluator: E,
	) -> Self {
		Self {
			flow,
			operators: HashMap::new(),
			versioned,
			unversioned,
			cdc,
			evaluator,
		}
	}

	pub fn initialize(&mut self) -> crate::Result<()> {
		// Initialize operator for all nodes
		let node_ids: Vec<NodeId> = self.flow.get_all_nodes().collect();

		for node_id in node_ids {
			if let Some(node) = self.flow.get_node(&node_id) {
				match &node.ty {
					NodeType::SourceTable {
						..
					} => {
						// Tables use VersionedStorage
						// directly
					}
					NodeType::Operator {
						operator,
					} => {
						// Create operator and context
						let op = self.create_operator(
							operator,
						)?;
						self.operators.insert(
							node_id.clone(),
							op,
						);
						// self.contexts.insert(
						// 	node_id.clone(),
						// 	OperatorContext::new(
						// 		&self.evaluator,
						// 	),
						// );
					}
					NodeType::SinkView {
						..
					} => {
						// Views use VersionedStorage
						// directly
					}
				}
			}
		}

		Ok(())
	}

	pub fn process_change(
		&self,
		node_id: &NodeId,
		change: Change,
	) -> crate::Result<()> {
		// let mut tx = ;

		let mut txn = ActiveCommandTransaction::new(
			self.versioned.begin_command()?,
			self.unversioned.clone(),
			self.cdc.clone(),
		);

		self.process_change_with_tx(&mut txn, node_id, change)?;
		txn.commit()?;

		Ok(())
	}

	fn process_change_with_tx(
		&self,
		txn: &mut ActiveCommandTransaction<T>,
		node_id: &NodeId,
		change: Change,
	) -> crate::Result<()> {
		let (node_type, output_nodes) =
			if let Some(node) = self.flow.get_node(node_id) {
				(node.ty.clone(), node.outputs.clone())
			} else {
				return Ok(()); // Node not found, nothing to do
			};

		let output_change = match &node_type {
			NodeType::SourceTable {
				..
			} => {
				// Source are handled elsewhere in the
				// system - just propagate
				change
			}
			NodeType::Operator {
				operator,
			} => {
				// Process through operator
				let transformed_diff = if let (
					Some(op),
					// Some(context),
				) = (
					self.operators.get(node_id),
					// self.contexts.get(node_id),
				) {
					op.apply(
						&OperatorContext::new(
							&self.evaluator,
						),
						change,
					)?
				} else {
					panic!("Operator or context not found");
				};

				// Stateful operator need to persist
				// their internal state
				// if operator.is_stateful() {
				// 	self.apply_to_view(txn, node_id,
				// &transformed_diff)?; }

				transformed_diff
			}
			NodeType::SinkView {
				view,
				..
			} => {
				// Sinks persist the final results
				self.apply_to_view(txn, *view, &change)?;
				change
			}
		};

		// Propagate to downstream nodes
		for output_id in output_nodes {
			self.process_change_with_tx(
				txn,
				&output_id,
				output_change.clone(),
			)?;
		}

		Ok(())
	}

	pub fn hack(
		&self,
		flow: &Flow,
		txn: &mut ActiveCommandTransaction<T>,
		node_id: &NodeId,
		change: Change,
	) -> crate::Result<()> {
		let mut operators: HashMap<
			NodeId,
			Box<dyn Operator<E> + Send + Sync + 'static>,
		> = HashMap::new();
		let mut contexts: HashMap<NodeId, OperatorContext<E>> =
			HashMap::new();

		// Initialize operator for all nodes
		let node_ids: Vec<NodeId> = flow.get_all_nodes().collect();

		for node_id in node_ids {
			if let Some(node) = flow.get_node(&node_id) {
				match &node.ty {
					NodeType::SourceTable {
						..
					} => {
						// Tables use VersionedStorage
						// directly
					}
					NodeType::Operator {
						operator,
					} => {
						// Create operator and context
						let op = self.create_operator(
							operator,
						)?;
						operators.insert(
							node_id.clone(),
							op,
						);
						contexts.insert(
							node_id.clone(),
							OperatorContext::new(
								&self.evaluator,
							),
						);
					}
					NodeType::SinkView {
						..
					} => {
						// Views use VersionedStorage
						// directly
					}
				}
			}
		}

		let (node_type, output_nodes) =
			if let Some(node) = flow.get_node(node_id) {
				(node.ty.clone(), node.outputs.clone())
			} else {
				return Ok(()); // Node not found, nothing to do
			};

		let output_change = match &node_type {
			NodeType::SourceTable {
				..
			} => {
				// Source are handled elsewhere in the
				// system - just propagate
				change
			}
			NodeType::Operator {
				operator,
			} => {
				// Process through operator
				let transformed_diff = if let (
					Some(op),
					Some(context),
				) = (
					operators.get(node_id),
					contexts.get(node_id),
				) {
					op.apply(context, change)?
				} else {
					panic!("Operator or context not found");
				};

				// Stateful operator need to persist
				// their internal state
				// if operator.is_stateful() {
				// 	self.persist_state(txn, node_id,
				// &transformed_diff)?; }

				transformed_diff
			}
			NodeType::SinkView {
				view,
				..
			} => {
				// Sinks persist the final results
				self.apply_to_view(txn, *view, &change)?;
				change
			}
		};

		// Propagate to downstream nodes
		for output_id in output_nodes {
			// self.process_change_with_tx(txn, &output_id,
			// output_change.clone())?;
			self.hack(
				flow,
				txn,
				&output_id,
				output_change.clone(),
			)?;
		}

		Ok(())
	}

	fn create_operator(
		&self,
		operator_type: &OperatorType,
	) -> crate::Result<Box<dyn Operator<E> + Send + Sync + 'static>> {
		match operator_type {
			OperatorType::Filter {
				predicate,
			} => Ok(Box::new(FilterOperator::new(
				predicate.clone(),
			))),
			OperatorType::Map {
				expressions,
			} => Ok(Box::new(MapOperator::new(expressions.clone()))),
			_ => {
				panic!(
					"Operator type {:?} not implemented yet",
					operator_type
				)
			}
		}
	}

	fn apply_to_view(
		&self,
		txn: &mut ActiveCommandTransaction<T>,
		view_id: ViewId,
		change: &Change,
	) -> crate::Result<()> {
		let layout = EncodedRowLayout::new(&[Type::Utf8, Type::Int1]);

		let view = ViewDef {
			id: ViewId(view_id.0),
			schema: SchemaId(0),
			name: "view".to_string(),
			columns: vec![
				ViewColumnDef {
					id: ViewColumnId(0),
					name: "name".to_string(),
					ty: Type::Utf8,
					index: ColumnIndex(0),
				},
				ViewColumnDef {
					id: ViewColumnId(1),
					name: "age".to_string(),
					ty: Type::Int1,
					index: ColumnIndex(1),
				},
			],
		};

		for diff in &change.diffs {
			match diff {
				Diff::Insert {
					after,
				} => {
					// Convert columns to row deltas
					// let columns_deltas =
					// self.columns_to_deltas(columns,
					// node_id)?;
					// deltas.extend(columns_deltas);

					let row_count = after.row_count();

					for row_idx in 0..row_count {
						// if !mask.get(row_idx) {
						//     continue;
						// }

						let mut row =
							layout.allocate_row();

						// For each table column, find
						// if it exists in the input
						// columns
						for (view_idx, view_column) in
							view.columns
								.iter()
								.enumerate()
						{
							let value = if let Some(input_column) =
                                after.iter().find(|col| col.name() == view_column.name)
                            {
                                input_column.data().get_value(row_idx)
                            } else {
                                Value::Undefined
                            };

							// let policies:
							// Vec<ColumnPolicyKind>
							// =
							//     table_column.
							// policies.iter().
							// map(|cp| cp.policy.
							// clone()).collect();
							//
							// value = coerce_value_to_column_type(
							//     value,
							//     table_column.ty,
							//     ColumnDescriptor::new()
							//         .with_schema(&schema.name)
							//         .with_table(&
							// table.name)
							//         .with_column(&table_column.name)
							//         .with_column_type(table_column.ty)
							//         .with_policies(policies),
							// )?;

							match value {
                                Value::Bool(v) => layout.set_bool(&mut row, view_idx, v),
                                Value::Float4(v) => layout.set_f32(&mut row, view_idx, *v),
                                Value::Float8(v) => layout.set_f64(&mut row, view_idx, *v),
                                Value::Int1(v) => layout.set_i8(&mut row, view_idx, v),
                                Value::Int2(v) => layout.set_i16(&mut row, view_idx, v),
                                Value::Int4(v) => layout.set_i32(&mut row, view_idx, v),
                                Value::Int8(v) => layout.set_i64(&mut row, view_idx, v),
                                Value::Int16(v) => layout.set_i128(&mut row, view_idx, v),
                                Value::Utf8(v) => layout.set_utf8(&mut row, view_idx, v),
                                Value::Uint1(v) => layout.set_u8(&mut row, view_idx, v),
                                Value::Uint2(v) => layout.set_u16(&mut row, view_idx, v),
                                Value::Uint4(v) => layout.set_u32(&mut row, view_idx, v),
                                Value::Uint8(v) => layout.set_u64(&mut row, view_idx, v),
                                Value::Uint16(v) => layout.set_u128(&mut row, view_idx, v),
                                Value::Date(v) => layout.set_date(&mut row, view_idx, v),
                                Value::DateTime(v) => layout.set_datetime(&mut row, view_idx, v),
                                Value::Time(v) => layout.set_time(&mut row, view_idx, v),
                                Value::Interval(v) => layout.set_interval(&mut row, view_idx, v),
                                Value::RowId(_v) => {}
                                Value::IdentityId(v) => layout.set_identity_id(&mut row, view_idx, v),
                                Value::Uuid4(v) => layout.set_uuid4(&mut row, view_idx, v),
                                Value::Uuid7(v) => layout.set_uuid7(&mut row, view_idx, v),
                                Value::Blob(v) => layout.set_blob(&mut row, view_idx, &v),
                                Value::Undefined => layout.set_undefined(&mut row, view_idx),
                            }
						}

						// Insert the row into the
						// database
						let row_id = ViewRowSequence::next_row_id(txn, ViewId(view_id.0))?;

						txn.set(
							&ViewRowKey { view: ViewId(view_id.0), row: row_id }.encode(),
							row,
                        )
                        .unwrap();

						// inserted_count += 1;
					}
				}
				Diff::Update {
					before: _,
					after: _,
				} => {
					// For updates, we could implement a
					// more sophisticated approach
					// For now, just insert the new columns
					// let columns_deltas =
					// self.columns_to_deltas(new,
					// node_id)?;
					// deltas.extend(columns_deltas);
					todo!()
				}
				Diff::Remove {
					before: _,
				} => {
					// Convert columns to remove deltas
					// let columns_deltas =
					// self.columns_to_remove_deltas(columns,
					// node_id)?;
					// deltas.extend(columns_deltas);
					todo!()
				}
			}
		}

		Ok(())
	}

	pub fn get_view_data(&self, view_name: &str) -> crate::Result<Columns> {
		// Find view node and read from versioned storage
		for node_id in self.flow.get_all_nodes() {
			if let Some(node) = self.flow.get_node(&node_id) {
				if let NodeType::SinkView {
					name,
					..
				} = &node.ty
				{
					if name == view_name {
						return self
							.read_columns_from_storage(
								&node_id,
							);
					}
				}
			}
		}
		panic!("View {} not found", view_name);
	}

	fn read_columns_from_storage(
		&self,
		node_id: &NodeId,
	) -> crate::Result<Columns> {
		// Start a read transaction
		let mut rx = self.versioned.begin_query()?;

		// Find the view_id from the node
		let view_id = if let Some(node) = self.flow.get_node(node_id) {
			if let NodeType::SinkView {
				view,
				..
			} = &node.ty
			{
				*view
			} else {
				// return Err(crate::Error::UnexpectedError("
				// Node is not a SinkView".to_string()));
				panic!()
			}
		} else {
			// return Err(crate::Error::UnexpectedError("Node not
			// found".to_string()));
			panic!()
		};

		let range = ViewRowKeyRange {
			view: view_id,
		};
		let versioned_data = rx
			.range(EncodedKeyRange::new(
				Included(range.start().unwrap()),
				Included(range.end().unwrap()),
			))
			.unwrap();

		let layout = EncodedRowLayout::new(&[Type::Utf8, Type::Int1]);

		let table = TableDef {
			id: TableId(node_id.0),
			schema: SchemaId(0),
			name: "view".to_string(),
			columns: vec![
				TableColumnDef {
					id: TableColumnId(0),
					name: "name".to_string(),
					ty: Type::Utf8,
					policies: vec![],
					index: ColumnIndex(0),
					auto_increment: false,
				},
				TableColumnDef {
					id: TableColumnId(1),
					name: "age".to_string(),
					ty: Type::Int1,
					policies: vec![],
					index: ColumnIndex(1),
					auto_increment: false,
				},
			],
		};

		let mut columns = Columns::from_table_def(&table);
		let mut iter = versioned_data.into_iter();
		while let Some(versioned) = iter.next() {
			columns.append_rows(&layout, [versioned.row])?;
		}
		Ok(columns)
	}
}
