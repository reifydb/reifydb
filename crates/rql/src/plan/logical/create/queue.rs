// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_catalog::{
	catalog::queue::QueueColumnToCreate,
	error::{CatalogError, CatalogObjectKind},
};
use reifydb_core::interface::catalog::queue::{Queue, QueueDeduplicate, QueueDispatch, QueueRetention, QueueRetry};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{
	fragment::Fragment,
	value::{
		constraint::{Constraint, TypeConstraint},
		duration::Duration,
	},
};

use crate::{
	Result,
	ast::ast::{
		AstColumnProperty, AstCreateQueue, AstQueueDeduplicate, AstQueueDispatch, AstQueueRetention,
		AstQueueRetry,
	},
	convert_data_type_with_constraints,
	duration::{DurationBound, FOREVER, compile_duration, invalid_option},
	plan::logical::{
		Compiler, CreateQueueNode, LogicalPlan,
		time_domain::{TimeDeclaration, resolve_declared_source_time},
	},
	token::token::Token,
};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_create_queue(
		&self,
		ast: AstCreateQueue<'bump>,
		tx: &mut Transaction<'_>,
	) -> Result<LogicalPlan<'bump>> {
		let mut columns: Vec<QueueColumnToCreate> = vec![];

		let queue_ns_segments: Vec<&str> = ast.queue.namespace.iter().map(|n| n.text()).collect();

		for col in ast.columns.into_iter() {
			let mut constraint = convert_data_type_with_constraints(&col.ty)?;

			let name = col.name.to_owned();
			let ty_fragment = col.ty.name_fragment().to_owned();
			let fragment = Fragment::merge_all([name.clone(), ty_fragment]);

			let mut auto_increment = false;
			let mut dictionary_id = None;
			let properties = vec![];

			for property in &col.properties {
				match property {
					AstColumnProperty::AutoIncrement => auto_increment = true,
					AstColumnProperty::Dictionary(dict_ident) => {
						let dict_ns_segments: Vec<&str> = if dict_ident.namespace.is_empty() {
							queue_ns_segments.clone()
						} else {
							dict_ident.namespace.iter().map(|n| n.text()).collect()
						};
						let dict_name = dict_ident.name.text();

						let Some(namespace) = self
							.catalog
							.find_namespace_by_segments(tx, &dict_ns_segments)?
						else {
							return Err(CatalogError::NotFound {
								kind: CatalogObjectKind::Namespace,
								namespace: dict_ns_segments.join("::"),
								name: dict_name.to_string(),
								fragment: dict_ident.name.to_owned(),
							}
							.into());
						};

						let Some(dictionary) = self.catalog.find_dictionary_by_name(
							tx,
							namespace.id(),
							dict_name,
						)?
						else {
							return Err(CatalogError::NotFound {
								kind: CatalogObjectKind::Dictionary,
								namespace: dict_ns_segments.join("::"),
								name: dict_name.to_string(),
								fragment: dict_ident.name.to_owned(),
							}
							.into());
						};

						dictionary_id = Some(dictionary.id);

						constraint = TypeConstraint::with_constraint(
							constraint.get_type(),
							Constraint::Dictionary(dictionary.id, dictionary.id_type),
						);
					}
					AstColumnProperty::Saturation(_) => {}
					AstColumnProperty::Default(_) => {}
				}
			}

			columns.push(QueueColumnToCreate {
				name,
				fragment,
				constraint,
				properties,
				auto_increment,
				dictionary_id,
			});
		}

		let dispatch = compile_dispatch(&ast.dispatch, &columns, &queue_ns_segments)?;
		let deduplicate = compile_deduplicate(ast.deduplicate.as_ref(), &columns, &queue_ns_segments)?;
		let retention = compile_retention(ast.retention.as_ref())?;
		let retry = compile_retry(ast.retry.as_ref())?;

		let time = resolve_declared_source_time(
			&TimeDeclaration::from(&ast.time_declaration),
			columns.iter().map(|c| (c.name.text(), c.constraint.get_type())),
			None,
		)?;

		Ok(LogicalPlan::CreateQueue(CreateQueueNode {
			queue: ast.queue,
			if_not_exists: false,
			columns,
			dispatch,
			deduplicate,
			retention,
			retry,
			time,
		}))
	}
}

fn compile_dispatch(
	ast: &AstQueueDispatch<'_>,
	columns: &[QueueColumnToCreate],
	namespace: &[&str],
) -> Result<QueueDispatch> {
	match ast {
		AstQueueDispatch::Fifo(fifo) => {
			let partitions = match &fifo.partitions {
				Some(token) => compile_partitions(token)?,
				None => Queue::DEFAULT_PARTITIONS,
			};

			let ordered_by = match &fifo.ordered_by {
				Some(token) => Some(compile_column_reference(token, columns, namespace)?),
				None => None,
			};

			Ok(QueueDispatch::Fifo {
				partitions,
				ordered_by,
			})
		}
	}
}

fn compile_column_reference(token: &Token<'_>, columns: &[QueueColumnToCreate], namespace: &[&str]) -> Result<String> {
	let column = token.fragment.text();
	if !columns.iter().any(|c| c.name.text() == column) {
		return Err(CatalogError::NotFound {
			kind: CatalogObjectKind::Column,
			namespace: namespace.join("::"),
			name: column.to_string(),
			fragment: token.fragment.to_owned(),
		}
		.into());
	}
	Ok(column.to_string())
}

fn compile_partitions(token: &Token<'_>) -> Result<u16> {
	let text = token.fragment.text();
	let parsed = text
		.parse::<u32>()
		.ok()
		.filter(|value| *value >= Queue::MIN_PARTITIONS as u32 && *value <= Queue::MAX_PARTITIONS as u32);

	match parsed {
		Some(value) => Ok(value as u16),
		None => Err(invalid_option(
			token,
			&format!("'partitions' between {} and {}", Queue::MIN_PARTITIONS, Queue::MAX_PARTITIONS),
		)),
	}
}

fn compile_deduplicate(
	ast: Option<&AstQueueDeduplicate<'_>>,
	columns: &[QueueColumnToCreate],
	namespace: &[&str],
) -> Result<Option<QueueDeduplicate>> {
	let Some(ast) = ast else {
		return Ok(None);
	};

	if ast.by.is_empty() {
		return Err(invalid_option(&ast.token, "'by' with at least one column"));
	}

	let mut by = Vec::with_capacity(ast.by.len());
	for token in &ast.by {
		let column = token.fragment.text();
		if !columns.iter().any(|c| c.name.text() == column) {
			return Err(CatalogError::NotFound {
				kind: CatalogObjectKind::Column,
				namespace: namespace.join("::"),
				name: column.to_string(),
				fragment: token.fragment.to_owned(),
			}
			.into());
		}
		if by.iter().any(|existing| existing == column) {
			return Err(invalid_option(token, "'by' without a repeated column"));
		}
		by.push(column.to_string());
	}

	let ttl = match &ast.ttl {
		Some(token) if token.fragment.text() == FOREVER => Duration::MAX,
		Some(token) => compile_duration(token, DurationBound::Positive, "'ttl'")?,
		None => Duration::MAX,
	};

	Ok(Some(QueueDeduplicate {
		by,
		ttl,
	}))
}

fn compile_retention(ast: Option<&AstQueueRetention<'_>>) -> Result<QueueRetention> {
	let Some(ast) = ast else {
		return Ok(QueueRetention::default());
	};

	let done = match &ast.done {
		Some(token) => Some(compile_duration(token, DurationBound::Positive, "'done'")?),
		None => None,
	};

	Ok(QueueRetention {
		done,
	})
}

fn compile_retry(ast: Option<&AstQueueRetry<'_>>) -> Result<QueueRetry> {
	let Some(ast) = ast else {
		return Ok(QueueRetry::default());
	};

	let attempts = match &ast.attempts {
		Some(token) => match token.fragment.text().parse::<u32>().ok().filter(|value| *value >= 1) {
			Some(value) => value,
			None => return Err(invalid_option(token, "'attempts' of at least 1")),
		},
		None => Queue::DEFAULT_RETRY_ATTEMPTS,
	};

	let backoff = match &ast.backoff {
		Some(token) => compile_duration(token, DurationBound::Positive, "'backoff'")?,
		None => Queue::DEFAULT_RETRY_BACKOFF,
	};

	Ok(QueueRetry {
		attempts,
		backoff,
	})
}
