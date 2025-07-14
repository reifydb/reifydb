// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::ast::lex::lex;
use crate::ast::parse::parse;
use crate::ast::{Ast, AstFrom, AstJoin};
use reifydb_core::Error;

pub fn explain_ast(query: &str) -> Result<String, Error> {
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
    let data_type = match ast {
        Ast::Aggregate(_) => "Aggregate",
        Ast::Row(_) => "Row",
        Ast::Cast(_) => "Cast",
        Ast::Create(_) => "Create",
        Ast::Describe(_) => "Describe",
        Ast::Filter(_) => "Filter",
        Ast::From(_) => "From",
        Ast::Identifier(_) => "Identifier",
        Ast::Infix(_) => "Infix",
        Ast::AstInsert(_) => "Insert",
        Ast::Join(_) => "Join",
        Ast::List(_) => "List",
        Ast::Literal(_) => "Literal",
        Ast::Nop => "Nop",
        Ast::Sort(_) => "Order",
        Ast::Policy(_) => "Policy",
        Ast::PolicyBlock(_) => "PolicyBlock",
        Ast::Prefix(_) => "Prefix",
        Ast::Map(_) => "Map",
        Ast::Take(_) => "Take",
        Ast::Tuple(_) => "Tuple",
        Ast::DataType(_) => "DataType",
        Ast::Wildcard(_) => "Wildcard",
    };

    let branch = if is_last { "└──" } else { "├──" };
    
    // Special handling for Row to show field summary
    let description = match &ast {
        Ast::Row(r) => {
            let field_names: Vec<&str> = r.fields.iter().map(|f| f.key.value()).collect();
            format!("{} ({} fields: {})", data_type, r.fields.len(), field_names.join(", "))
        }
        _ => data_type.to_string()
    };
    
    output.push_str(&format!(
		"{}{} {} @ line {}, column {} — \"{}\"\n",
		prefix, branch, description, span.line.0, span.column.0, span.fragment
    ));

    let child_prefix = format!("{}{}", prefix, if is_last { "    " } else { "│   " });
    let mut children: Vec<Ast> = vec![];

    match ast {
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
            AstFrom::Static { list: query, .. } => {
                children.extend(query.nodes);
            }
        },
        Ast::Aggregate(a) => {
            children.extend(a.by);
            children.extend(a.map);
        }
        Ast::AstInsert(_) => {
            unimplemented!()
        }
        Ast::Join(AstJoin::LeftJoin { with, on, .. }) => {
            children.push(*with);
            children.extend(on);
        }
        Ast::Map(s) => children.extend(s.map),
        Ast::Sort(o) => {
            for col in &o.columns {
                children.push(Ast::Identifier(col.clone()));
            }
        }
        Ast::PolicyBlock(pb) => {
            children.extend(pb.policies.iter().map(|p| *p.value.clone()).collect::<Vec<_>>())
        }
        Ast::Policy(p) => children.push(*p.value),
        Ast::DataType(_) => {}
        Ast::Row(r) => {
            // Add each field as a child - they will be displayed as key: value pairs
            for field in &r.fields {
                // Create an infix node to represent "key: value" 
                let key_ast = Ast::Identifier(field.key.clone());
                let value_ast = *field.value.clone();
                children.push(key_ast);
                children.push(value_ast);
            }
        }
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
