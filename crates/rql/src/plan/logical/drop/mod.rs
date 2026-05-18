// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use crate::{
	Result,
	ast::ast::AstDrop,
	plan::logical::{
		Compiler, DropAuthenticationNode, DropBindingNode, DropDictionaryNode, DropHandlerNode,
		DropIdentityNode, DropNamespaceNode, DropPolicyNode, DropProcedureNode, DropRingBufferNode,
		DropRoleNode, DropSeriesNode, DropSinkNode, DropSourceNode, DropSubscriptionNode, DropSumTypeNode,
		DropTableNode, DropTestNode, DropViewNode, LogicalPlan,
	},
};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_drop(&self, ast: AstDrop<'bump>) -> Result<LogicalPlan<'bump>> {
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
			AstDrop::Identity(node) => Ok(LogicalPlan::DropIdentity(DropIdentityNode {
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
			AstDrop::Policy(node) => Ok(LogicalPlan::DropPolicy(DropPolicyNode {
				target_type: node.target_type,
				name: node.name,
				if_exists: node.if_exists,
			})),
			AstDrop::Source(node) => Ok(LogicalPlan::DropSource(DropSourceNode {
				source: node.source,
				if_exists: node.if_exists,
				cascade: node.cascade,
			})),
			AstDrop::Sink(node) => Ok(LogicalPlan::DropSink(DropSinkNode {
				sink: node.sink,
				if_exists: node.if_exists,
				cascade: node.cascade,
			})),
			AstDrop::Procedure(node) => Ok(LogicalPlan::DropProcedure(DropProcedureNode {
				procedure: node.procedure,
				if_exists: node.if_exists,
			})),
			AstDrop::Handler(node) => Ok(LogicalPlan::DropHandler(DropHandlerNode {
				handler: node.handler,
				if_exists: node.if_exists,
			})),
			AstDrop::Test(node) => Ok(LogicalPlan::DropTest(DropTestNode {
				test: node.test,
				if_exists: node.if_exists,
			})),
			AstDrop::Binding(node) => Ok(LogicalPlan::DropBinding(DropBindingNode {
				binding: node.binding,
				if_exists: node.if_exists,
			})),
		}
	}
}
