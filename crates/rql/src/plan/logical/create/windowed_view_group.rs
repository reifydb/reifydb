// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::interface::catalog::ringbuffer::EvictionVisibility;
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{fragment::Fragment, value::duration::Duration};

use crate::{
	Result,
	ast::ast::{
		Ast, AstCreateDeferredWindowedViewGroup, AstOperatorWith, AstOperatorWithEntry, AstOperatorWithKey,
		AstOperatorWithValue, AstStatement, AstWindowKind, AstWindowedViewGroupMember,
	},
	duration::{DurationBound, compile_duration, invalid_option},
	error::RqlError,
	plan::logical::{
		Compiler, CreateDeferredWindowedViewGroupNode, LogicalPlan, WindowedViewGroupMember,
		create::check_view_partition,
	},
};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_deferred_windowed_view_group(
		&self,
		ast: AstCreateDeferredWindowedViewGroup<'bump>,
		tx: &mut Transaction<'_>,
	) -> Result<LogicalPlan<'bump>> {
		if ast.members.is_empty() {
			return Err(RqlError::WindowedViewGroupNoMembers {
				fragment: ast.group.name.to_owned(),
			}
			.into());
		}

		let group_ns_segments: Vec<&str> = ast.group.namespace.iter().map(|n| n.text()).collect();
		let columns = self.compile_view_columns(ast.columns, &group_ns_segments, tx)?;
		check_view_partition(&columns, &ast.storage_kind, &group_ns_segments)?;

		let mut durations: Vec<Duration> = Vec::with_capacity(ast.members.len());
		let mut members = Vec::with_capacity(ast.members.len());
		for (member, mut as_clause) in ast.members.into_iter().zip(ast.as_clauses) {
			let duration = member_duration(&member.duration)?;
			if durations.contains(&duration) {
				return Err(RqlError::WindowedViewGroupDuplicateDuration {
					duration: duration.to_string(),
					fragment: entry_fragment(&member.duration),
				}
				.into());
			}
			durations.push(duration);

			inject_member(&mut as_clause, &member)?;
			let as_clause = self.compile(as_clause, tx)?;

			let (ttl, persistent, ttl_eviction) = match member.settings {
				Some(settings) => (
					settings.ttl.map(Self::compile_ttl).transpose()?,
					settings.persistent.is_none_or(|p| p.value),
					settings.ttl_eviction,
				),
				None => (None, true, EvictionVisibility::Silent),
			};

			members.push(WindowedViewGroupMember {
				view: member.view,
				duration,
				ttl,
				persistent,
				ttl_eviction,
				as_clause,
			});
		}

		Ok(LogicalPlan::CreateDeferredWindowedViewGroup(CreateDeferredWindowedViewGroupNode {
			group: ast.group,
			columns,
			storage_kind: ast.storage_kind,
			members,
		}))
	}
}

fn member_duration(entry: &AstOperatorWithEntry<'_>) -> Result<Duration> {
	match &entry.value {
		Some(AstOperatorWithValue::Literal(token)) | Some(AstOperatorWithValue::Word(token)) => {
			compile_duration(token, DurationBound::Positive, "a windowed view group member")
		}
		_ => match &entry.key {
			AstOperatorWithKey::Word(token) => {
				Err(invalid_option(token, "a bare duration literal such as `1m` for a member"))
			}
			AstOperatorWithKey::Column(_) => unreachable!("member keys are parsed as words"),
		},
	}
}

fn entry_fragment(entry: &AstOperatorWithEntry<'_>) -> Fragment {
	match &entry.value {
		Some(AstOperatorWithValue::Literal(token)) | Some(AstOperatorWithValue::Word(token)) => {
			token.fragment.to_owned()
		}
		_ => entry.key.fragment(),
	}
}

fn inject_member<'bump>(as_clause: &mut AstStatement<'bump>, member: &AstWindowedViewGroupMember<'bump>) -> Result<()> {
	let windowed: Vec<usize> =
		as_clause.nodes.iter().enumerate().filter(|(_, node)| is_windowed(node)).map(|(i, _)| i).collect();
	let index = match windowed.as_slice() {
		[] => return Ok(()),
		[index] => *index,
		[_, second, ..] => {
			return Err(RqlError::WindowedViewGroupRequiresOneWindow {
				fragment: as_clause.nodes[*second].token().fragment.to_owned(),
			}
			.into());
		}
	};

	let with = match &mut as_clause.nodes[index] {
		Ast::Apply(apply) => {
			let with = apply.with.as_mut().expect("a windowed apply carries a with block");
			if !window_entry_is_tumbling_or_rolling(with) {
				unimplemented!("windowed view groups support tumbling and rolling windows only");
			}
			with
		}
		Ast::Window(window) => {
			if !matches!(window.kind, AstWindowKind::Tumbling | AstWindowKind::Rolling) {
				unimplemented!("windowed view groups support tumbling and rolling windows only");
			}
			window.with.get_or_insert_with(|| AstOperatorWith {
				token: member.token.clone(),
				entries: Vec::new(),
			})
		}
		_ => unreachable!("is_windowed matched only apply and window nodes"),
	};

	for entry in &with.entries {
		match key_text(entry) {
			Some("duration") => {
				return Err(RqlError::WindowedViewGroupWindowHasDuration {
					fragment: entry.key.fragment(),
				}
				.into());
			}
			Some("immutable") => {
				return Err(RqlError::WindowedViewGroupWindowHasImmutable {
					fragment: entry.key.fragment(),
				}
				.into());
			}
			Some("pane") => {
				return Err(RqlError::WindowedViewGroupWindowHasPane {
					fragment: entry.key.fragment(),
				}
				.into());
			}
			Some("slots") => unimplemented!("windowed view groups support time-sized windows only"),
			_ => {}
		}
	}

	with.entries.push(member.duration.clone());
	if let Some(pane) = &member.pane {
		with.entries.push(pane.clone());
	}
	if let Some(immutable) = &member.immutable {
		with.entries.push(immutable.clone());
	}
	Ok(())
}

fn is_windowed(node: &Ast<'_>) -> bool {
	match node {
		Ast::Window(_) => true,
		Ast::Apply(apply) => apply
			.with
			.as_ref()
			.is_some_and(|with| with.entries.iter().any(|e| key_text(e) == Some("window"))),
		_ => false,
	}
}

fn window_entry_is_tumbling_or_rolling(with: &AstOperatorWith<'_>) -> bool {
	with.entries.iter().any(|entry| {
		key_text(entry) == Some("window")
			&& matches!(&entry.value, Some(AstOperatorWithValue::Word(token)) if {
				let kind = token.fragment.text();
				kind.eq_ignore_ascii_case("tumbling") || kind.eq_ignore_ascii_case("rolling")
			})
	})
}

fn key_text<'a>(entry: &'a AstOperatorWithEntry<'_>) -> Option<&'a str> {
	entry.key.word()
}
