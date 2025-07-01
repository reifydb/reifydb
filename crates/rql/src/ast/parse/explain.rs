// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::ast::lex::lex;
use crate::ast::parse::parse;
use crate::ast::{Ast, AstFrom, AstJoin};
use reifydb_core::Error;

pub(crate) fn explain_ast(query: &str) -> Result<String, Error> {
    let token = lex(query).unwrap();
    let statements = parse(token).unwrap();

    let mut result = String::new();
    for statement in statements {
        for ast in statement {
            let mut output = String::new();
            render_ast_tree_inner(ast, "", true, &mut output);
            result += output.as_str();
        }
    }
    Ok(result)
}

fn render_ast_tree_inner(ast: Ast, prefix: &str, is_last: bool, output: &mut String) {
    let token = ast.token();
    let span = &token.span;
    let kind = match ast {
        Ast::Aggregate(_) => "Aggregate",
        Ast::Block(_) => "Block",
        Ast::Cast(_) => "Cast",
        Ast::Create(_) => "Create",
        Ast::Describe(_) => "Describe",
        Ast::Filter(_) => "Filter",
        Ast::From(_) => "From",
        Ast::Identifier(_) => "Identifier",
        Ast::Infix(_) => "Infix",
        Ast::Insert(_) => "Insert",
        Ast::Join(_) => "Join",
        Ast::Limit(_) => "Limit",
        Ast::Literal(_) => "Literal",
        Ast::Nop => "Nop",
        Ast::Order(_) => "Order",
        Ast::Policy(_) => "Policy",
        Ast::PolicyBlock(_) => "PolicyBlock",
        Ast::Prefix(_) => "Prefix",
        Ast::Select(_) => "Select",
        Ast::Tuple(_) => "Tuple",
        Ast::Kind(_) => "Kind",
        Ast::Wildcard(_) => "Wildcard",
    };

    let branch = if is_last { "└──" } else { "├──" };
    output.push_str(&format!(
        "{}{} {} @ line {}, offset {} — \"{}\"\n",
        prefix, branch, kind, span.line.0, span.offset.0, span.fragment
    ));

    let child_prefix = format!("{}{}", prefix, if is_last { "    " } else { "│   " });
    let mut children: Vec<Ast> = vec![];

    match ast {
        Ast::Block(b) => children.extend(b.nodes),
        Ast::Tuple(t) => children.extend(t.nodes),
        Ast::Prefix(p) => children.push(*p.node),
        Ast::Cast(c) => children.extend(c.tuple.nodes),
        Ast::Filter(f) => children.push(*f.node),
        Ast::From(from) => match from {
            AstFrom::Table { schema, table, .. } => {
                if let Some(schema) = schema {
                    children.push(Ast::Identifier(schema));
                }
                children.push(Ast::Identifier(table));
            }
            AstFrom::Query { query, .. } => {
                children.extend(query.nodes);
            }
        },
        Ast::Aggregate(a) => {
            children.extend(a.by);
            children.extend(a.select);
        }
        Ast::Insert(i) => {
            children.extend(i.columns.nodes);
            for row in &i.rows {
                children.extend(row.nodes.clone());
            }
        }
        Ast::Join(AstJoin::LeftJoin { with, on, .. }) => {
            children.push(*with);
            children.extend(on);
        }
        Ast::Select(s) => children.extend(s.select),
        Ast::Order(o) => {
            for col in &o.columns {
                children.push(Ast::Identifier(col.clone()));
            }
        }
        Ast::PolicyBlock(pb) => {
            children.extend(pb.policies.iter().map(|p| *p.value.clone()).collect::<Vec<_>>())
        }
        Ast::Policy(p) => children.push(*p.value),
        Ast::Kind(_) => {}
        Ast::Infix(i) => {
            children.push(*i.left);
            children.push(*i.right);
        }
        _ => {}
    }

    for (i, child) in children.iter().enumerate() {
        let last = i == children.len() - 1;
        render_ast_tree_inner(child.clone(), &child_prefix, last, output);
    }
}
