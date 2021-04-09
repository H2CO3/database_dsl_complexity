use sqlparser::ast::*;
use crate::visitor::{ Visitor, Acceptor };

/// Visitor for computing the "weighted nodes in the AST" metric.
#[derive(Clone, Default, Debug)]
struct WeightedNodeCountVisitor {
    /// State for accumulating the complexity metric.
    complexity: usize,
}

impl Visitor for WeightedNodeCountVisitor {
    fn visit_query(&mut self, query: &Query) {
        // FROM, WHERE, GROUP BY, and ORDER BY clauses are
        // all worth 2; whereas `HAVING` is an order of
        // magnitude less frequent (occurrences vs ~5000
        // `GROUP BY`, 12 000 `WHERE`, 17 000 `FROM` and
        // 19 000 `SELECT` clauses), with a score of 3.
        //
        // `LIMIT` is the equivalent of T-SQL's `TOP` or
        // `FETCH`, which bear a complexity of 2 or 3,
        // respectively (the sum of their frequencies
        // would also have a score of 2), whereas the
        // construct often used for offsetting, `ROW_NUMBER`,
        // has a complexity of 3.
        self.complexity += 2;

        if query.order_by.len() > 0 {
            self.complexity += 2;
        }
        if query.limit.is_some() || query.fetch.is_some() {
            self.complexity += 2;
        }
        if query.offset.is_some() {
            self.complexity += 3;
        }

        // `FROM`, `WHERE`, `GROUP BY`, and `HAVING`
        // are only available on `Select`, not `Query`.
        // See `visit_select()` for the code handling those ones.
    }

    fn visit_with(&mut self, with: &With) {
        // Plain `WITH` has a weight of 3.
        //
        // Recursion is only found in 9 queries of the corpus.
        // Pure lexical analysis could not find any instances
        // of it, though -- `RECURSIVE` never occurs outside
        // comments and the name of a stored procedure, and a
        // similar, although non-standard keyword, `CONNECT`
        // (part of the `CONNECT BY` clause) only ever occurs
        // as an identifier. Instead, parsing and a heuristic
        // method was applied to the ASTs of each query; this
        // identified 9, potentially recursive queries, of which
        // 8 turned out to actually be recursive. Their IDs are:
        // 27788
        // 28802
        // 28803
        // 222699
        // 339838
        // 490126
        // 715388
        // 969344
        // One identified query, #295447, turned out to be
        // non-recursive, a false positive.
        //
        // Based on these observations, recursion is classified
        // into the rarest-occurring cluster, and as such, it is
        // assigned a score of 8.
        let weight = if with.recursive {
            8
        } else {
            3
        };

        self.complexity += weight;
    }

    fn visit_cte(&mut self, _cte: &Cte) {
        self.complexity += 1;
    }

    fn visit_set_op(
        &mut self,
        op: SetOperator,
        _all: bool,
        _left: &SetExpr,
        _right: &SetExpr,
    ) {
        // UNION is surprisingly more common than the other two operators.
        self.complexity += match op {
            SetOperator::Union => 2,
            SetOperator::Intersect => 8,
            SetOperator::Except => 8,
        };
    }

    fn visit_values(&mut self, _values: &Values) {
        self.complexity += 3;
    }

    fn visit_select(&mut self, select: &Select) {
        self.complexity += 2;

        // `FROM`, `WHERE`, `GROUP BY`, and `HAVING`
        // are only available from here, not from `Query`.
        if select.from.len() > 0 {
            self.complexity += 2;
        }
        if select.selection.is_some() {
            self.complexity += 2;
        }
        if select.group_by.len() > 0 {
            self.complexity += 2;
        }
        if select.having.is_some() {
            self.complexity += 3;
        }
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
        self.complexity += 1; // `AS` has a score of 1, too
    }

    fn visit_object_name(&mut self, _name: &ObjectName) {
        self.complexity += 1;
    }

    fn visit_join(&mut self, join: &Join) {
        // `INNER JOIN` is the most fequent kind (the two keywords
        // appear approximately 4000 and 8000 times, respectively).
        // `LEFT` and `RIGHT` approx. 700 and 1500 times, respectively.
        // `FULL` appears exactly 100 times, being the rarest kind of
        // join, closely followed by `APPLY` (109 occurrences) and by
        // `CROSS` (~160).
        self.complexity += match join.join_operator {
            JoinOperator::Inner(_) => 2,

            | JoinOperator::LeftOuter(_)
            | JoinOperator::RightOuter(_) => 3,

            | JoinOperator::FullOuter(_)
            | JoinOperator::CrossJoin
            | JoinOperator::CrossApply
            | JoinOperator::OuterApply => 5,
        };
    }

    fn visit_unary_op(&mut self, op: &Expr) {
        self.complexity += match *op {
            // `IS` and `NULL` are both worth 2 points so this is easy
            Expr::IsNull(_) | Expr::IsNotNull(_) => 2,
            Expr::Exists(_) => 3,
            Expr::UnaryOp { ref op, .. } => match *op {
                | UnaryOperator::Plus
                | UnaryOperator::Minus
                | UnaryOperator::Not => 2,
                _ => 1 // don't know about the rest
            },
            _ => unreachable!("not an unary operator: {:#?}", op)
        };
    }

    fn visit_binary_op(&mut self, op: &Expr) {
        use BinaryOperator::*;

        self.complexity += match *op {
            // `IN` is worth 3 points
            Expr::InList { .. } | Expr::InSubquery { .. } => 3,
            Expr::Cast { .. } => 2,
            Expr::BinaryOp { ref op, .. } => match *op {
                Plus | Minus | Multiply | Divide => 2,
                Eq | Gt => 2,
                And | Or => 2,
                Like | NotLike => 2,
                NotEq | LtEq | GtEq | Lt => 3,
                Modulus => 5, // `%` is apparently super uncommon
                _ => 1, // no information about the rest
            },
            _ => 1 // the rest is just function calls; parentheses get 1 mark
        }
    }

    fn visit_between(&mut self, _negated: bool) {
        self.complexity += 3;
    }

    fn visit_value(&mut self, _value: &Value) {
        self.complexity += 1; // identifiers and literals are the most common
    }

    fn visit_type(&mut self, ty: &DataType) {
        todo!("Unimplemented type: {:#?}", ty);
    }

    fn visit_case(
        &mut self,
        _discriminant: Option<&Expr>,
        _conditions: &[Expr],
        _results: &[Expr],
        _else_result: Option<&Expr>,
    ) {
        self.complexity += 2; // it seems pretty common
    }
}

/// Returns the number of nodes in the AST, weighted by their complexity.
pub fn weighted_node_count(stmt: &Statement) -> usize {
    let mut v = WeightedNodeCountVisitor::default();
    stmt.accept(&mut v);
    v.complexity
}
