//! We use the visitor pattern for reducing the number of times
//! we have to repeat matching on subexpressions & subpatterns.

use sqlparser::ast::*;

pub trait Visitor {
    fn visit_query(&mut self, query: &Query);
    fn visit_with(&mut self, with: &With);
    fn visit_cte(&mut self, cte: &Cte);
    fn visit_set_op(&mut self,
                    op: SetOperator,
                    all: bool,
                    left: &SetExpr,
                    right: &SetExpr);
    fn visit_values(&mut self, values: &Values);
    fn visit_select(&mut self, select: &Select);
    fn visit_ident(&mut self, ident: &Ident);
    fn visit_compound_ident(&mut self, idents: &[Ident]);
    fn visit_wildcard(&mut self);
    fn visit_table_alias(&mut self, alias: &TableAlias);
    fn visit_object_name(&mut self, name: &ObjectName);
    fn visit_join(&mut self, join: &Join);
    fn visit_unary_op(&mut self, op: &Expr);
    fn visit_binary_op(&mut self, op: &Expr);
    fn visit_between(&mut self, negated: bool);
    fn visit_value(&mut self, value: &Value);
    fn visit_type(&mut self, ty: &DataType);
    fn visit_case(
        &mut self,
        discriminant: Option<&Expr>,
        conditions: &[Expr],
        results: &[Expr],
        else_result: Option<&Expr>,
    );
}

pub trait Acceptor {
    fn accept<V: Visitor>(&self, visitor: &mut V);
}
