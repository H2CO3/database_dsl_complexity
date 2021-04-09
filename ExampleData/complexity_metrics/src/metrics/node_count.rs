use sqlparser::ast::*;
use crate::visitor::{ Visitor, Acceptor };

/// Visitor for computing the "total nodes in the AST" metric.
#[derive(Clone, Default, Debug)]
struct NodeCountVisitor {
    /// State for accumulating the complexity metric.
    complexity: usize,
}

impl Visitor for NodeCountVisitor {
    fn visit_query(&mut self, _query: &Query) {
        self.complexity += 1;
    }

    fn visit_with(&mut self, _with: &With) {
        self.complexity += 1;
    }

    fn visit_cte(&mut self, _cte: &Cte) {
        self.complexity += 1;
    }

    fn visit_set_op(
        &mut self,
        _op: SetOperator,
        _all: bool,
        _left: &SetExpr,
        _right: &SetExpr,
    ) {
        self.complexity += 1;
    }

    fn visit_values(&mut self, _values: &Values) {
        self.complexity += 1;
    }

    fn visit_select(&mut self, _select: &Select) {
        self.complexity += 1;
    }

    fn visit_ident(&mut self, _ident: &Ident) {
        self.complexity += 1;
    }

    fn visit_compound_ident(&mut self, _idents: &[Ident]) {
        self.complexity += 1;
    }

    fn visit_wildcard(&mut self) {
        self.complexity += 1;
    }

    fn visit_table_alias(&mut self, _alias: &TableAlias) {
        self.complexity += 1;
    }

    fn visit_object_name(&mut self, _name: &ObjectName) {
        self.complexity += 1;
    }

    fn visit_join(&mut self, _join: &Join) {
        self.complexity += 1;
    }

    fn visit_unary_op(&mut self, _op: &Expr) {
        self.complexity += 1;
    }

    fn visit_binary_op(&mut self, _op: &Expr) {
        self.complexity += 1;
    }

    fn visit_between(&mut self, _negated: bool) {
        self.complexity += 1;
    }

    fn visit_value(&mut self, _value: &Value) {
        self.complexity += 1;
    }

    fn visit_type(&mut self, _type: &DataType) {
        self.complexity += 1;
    }

    fn visit_case(
        &mut self,
        _discriminant: Option<&Expr>,
        _conditions: &[Expr],
        _results: &[Expr],
        _else_result: Option<&Expr>,
    ) {
        self.complexity += 1;
    }
}

/// Returns the total number of nodes in the AST.
pub fn node_count(stmt: &Statement) -> usize {
    let mut v = NodeCountVisitor::default();
    stmt.accept(&mut v);
    v.complexity
}
