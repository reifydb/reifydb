// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use crate::{
	ast::ast::AstDrop,
	plan::logical::{
		Compiler, DropAuthenticationNode, DropDictionaryNode, DropFlowNode, DropNamespaceNode,
		DropRingBufferNode, DropRoleNode, DropSecurityPolicyNode, DropSeriesNode, DropSubscriptionNode,
		DropSumTypeNode, DropTableNode, DropUserNode, DropViewNode, LogicalPlan,
	},
};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_drop(&self, ast: AstDrop<'bump>) -> crate::Result<LogicalPlan<'bump>> {
		match ast {
			AstDrop::Namespace(node) => Ok(LogicalPlan::DropNamespace(DropNamespaceNode {
				segments: node.namespace.segments,
				if_exists: node.if_exists,
				cascade: node.cascade,
			})),
			AstDrop::Table(node) => Ok(LogicalPlan::DropTable(DropTableNode {
				table: node.table,
				if_exists: node.if_exists,
				cascade: node.cascade,
			})),
			AstDrop::View(node) => Ok(LogicalPlan::DropView(DropViewNode {
				view: node.view,
				if_exists: node.if_exists,
				cascade: node.cascade,
			})),
			AstDrop::RingBuffer(node) => Ok(LogicalPlan::DropRingBuffer(DropRingBufferNode {
				ringbuffer: node.ringbuffer,
				if_exists: node.if_exists,
				cascade: node.cascade,
			})),
			AstDrop::Dictionary(node) => Ok(LogicalPlan::DropDictionary(DropDictionaryNode {
				dictionary: node.dictionary,
				if_exists: node.if_exists,
				cascade: node.cascade,
			})),
			AstDrop::Enum(node) => Ok(LogicalPlan::DropSumType(DropSumTypeNode {
				sumtype: node.sumtype,
				if_exists: node.if_exists,
				cascade: node.cascade,
			})),
			AstDrop::Flow(node) => Ok(LogicalPlan::DropFlow(DropFlowNode {
				flow: node.flow,
				if_exists: node.if_exists,
				cascade: node.cascade,
			})),
			AstDrop::Subscription(node) => Ok(LogicalPlan::DropSubscription(DropSubscriptionNode {
				identifier: node.identifier,
				if_exists: node.if_exists,
				cascade: node.cascade,
			})),
			AstDrop::Series(node) => Ok(LogicalPlan::DropSeries(DropSeriesNode {
				series: node.series,
				if_exists: node.if_exists,
				cascade: node.cascade,
			})),
			AstDrop::User(node) => Ok(LogicalPlan::DropUser(DropUserNode {
				name: node.name,
				if_exists: node.if_exists,
			})),
			AstDrop::Role(node) => Ok(LogicalPlan::DropRole(DropRoleNode {
				name: node.name,
				if_exists: node.if_exists,
			})),
			AstDrop::Authentication(node) => Ok(LogicalPlan::DropAuthentication(DropAuthenticationNode {
				user: node.user,
				if_exists: node.if_exists,
				method: node.method,
			})),
			AstDrop::SecurityPolicy(node) => Ok(LogicalPlan::DropSecurityPolicy(DropSecurityPolicyNode {
				target_type: node.target_type,
				name: node.name,
				if_exists: node.if_exists,
			})),
		}
	}
}
