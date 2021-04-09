//! Halstead complexity metrics

use std::collections::HashMap;
use std::fmt::Write;
use sqlparser::ast::*;
use crate::visitor::{ Visitor, Acceptor };

/// Our definition of "operators" in SQL.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum Operator {
    /// A Common table Expression
    Cte,
    /// A table alias (`table_name(column1, column2, ...)`)
    TableAlias,
    /// A set operation.
    Set {
        op: SetOperator,
        all: bool,
    },
    /// `VALUES (...), (...)`
    Values,
    /// The `SELECT ...` part of a SELECT clause, without the `FROM`.
    Select { distinct: bool },
    /// `FROM ...`
    From,
    /// `CASE ... WHEN ...`
    Case,
    /// `... IS NULL`
    IsNull,
    /// `... IS NOT NULL`
    IsNotNull,
    /// `EXISTS ...`
    Exists,
    /// Any other unary operator
    Unary(UnaryOperator),
    /// `IN ...` (list or subquery)
    In,
    /// `CAST(... AS ...)` type conversion
    Cast,
    /// Function call
    Function,
    /// Any other binary operator
    Binary(BinaryOperator),
    /// `... [NOT] BETWEEN ... AND ...`
    Between { negated: bool },
    /// `ORDER BY` clause
    OrderBy,
    /// `GROUP BY` clause
    GroupBy,
    /// `WHERE` clause
    Where,
    /// `HAVING` clause
    Having,
    /// `LIMIT` clause
    Limit,
    /// `OFFSET` clause
    Offset,
    /// `FETCH [FIRST | LAST] ... [ROWS]` clause
    Fetch,
    /// `[INNER] JOIN`
    InnerJoin,
    /// `LEFT [OUTER] JOIN`
    LeftJoin,
    /// `RIGHT [OUTER] JOIN`
    RightJoin,
    /// `[FULL] OUTER JOIN`
    OuterJoin,
    /// `CROSS JOIN`
    CrossJoin,
    /// `CROSS APPLY`
    CrossApply,
    /// `OUTER APPLY`
    OuterApply,
    /// `ON ...` JOIN constraint
    OnJoinConstraint,
    /// `USING ...` JOIN constraint
    UsingJoinConstraint,
    /// `NATURAL` JOIN constraint
    NaturalJoinConstraint,
}

/// Our definition of "operands" in SQL.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum Operand {
    /// An identifier.
    Ident(String),
    /// `*`, all columns.
    Wildcard,
    /// `NULL` literal.
    Null,
    /// A Boolean literal.
    Boolean(bool),
    /// A numeric literal.
    Number(String),
    /// A string literal.
    String(String),
}

/// Visitor for computing Halstead measures.
#[derive(Clone, Default, Debug)]
struct HalsteadVisitor {
    /// Accumulates the count of operators.
    operators: HashMap<Operator, usize>,
    /// Accumulates the count of operands.
    operands: HashMap<Operand, usize>,
}

impl HalsteadVisitor {
    /// Increment the number of operators.
    fn add_operator(&mut self, op: Operator) {
        *self.operators.entry(op).or_insert(0) += 1;
    }

    /// Increment the number of operands.
    fn add_operand(&mut self, op: Operand) {
        *self.operands.entry(op).or_insert(0) += 1;
    }
}

impl Visitor for HalsteadVisitor {
    fn visit_query(&mut self, query: &Query) {
        // `Query` is not a single operator, it represents
        // different parts of a `SELECT` in one type.
        // So do not add a separate `Operator::Select` here
        // (that will be handled by the visitor of the body,
        // which is a `SetExpr`).
        //
        // Do add operators for the different sub-clauses, however.
        // These (ORDER BY, LIMIT, OFFSET, FETCH) do not have
        // their own visitor methods, because they should not be
        // counted as separate AST nodes -- they are markers for
        // the actual children of a select, which is their inner
        // expression. However, they are operators on their own,
        // so it is useful to include them in the Halstead metric.
        if query.order_by.len() > 0 {
            // NB: we intentionally do not distinguish
            // between ASCENDING and DESCENDING sorts.
            self.add_operator(Operator::OrderBy);
        }
        if query.limit.is_some() {
            self.add_operator(Operator::Limit);
        }
        if query.offset.is_some() {
            self.add_operator(Operator::Offset);
        }
        if query.fetch.is_some() {
            // Similarly, we do not distinguish between
            // syntactically different forms of `FETCH`.
            // An argument could be made for doing so,
            // however.
            self.add_operator(Operator::Fetch);
        }
    }

    fn visit_with(&mut self, _with: &With) {
        // `With` is not a single operator, it represents
        // one or many CTEs.
        // So do not add a separate `Operator::With` here
        // -- the `visit_cte()` method will handle each
        // CTE on its own. The `WITH` clause merely makes
        // the distinction between there being any CTEs
        // or there being none, so it's not an operator
        // on its own right.
    }

    fn visit_cte(&mut self, _cte: &Cte) {
        self.add_operator(Operator::Cte);
    }

    fn visit_set_op(
        &mut self,
        op: SetOperator,
        all: bool,
        _left: &SetExpr,
        _right: &SetExpr,
    ) {
        self.add_operator(Operator::Set { op, all });
    }

    fn visit_values(&mut self, _values: &Values) {
        self.add_operator(Operator::Values);
    }

    fn visit_select(&mut self, select: &Select) {
        // We want to treat `SELECT` and `SELECT DISTINCT`
        // as different kinds of operators.
        self.add_operator(Operator::Select {
            distinct: select.distinct,
        });

        // See the comment in `visit_query()` for a description
        // of the disctinction between AST nodes and operators,
        // and why we explicitly visit components of the `Select`
        // whereas these components have no corresponding visitor
        // methods.
        if select.top.is_some() {
            todo!("SELECT TOP ... is not yet supported");
        }
        if select.from.len() > 0 {
            self.add_operator(Operator::From);
        }
        if select.selection.is_some() {
            self.add_operator(Operator::Where);
        }
        if select.group_by.len() > 0 {
            self.add_operator(Operator::GroupBy);
        }
        if select.having.is_some() {
            self.add_operator(Operator::Having);
        }
    }

    fn visit_ident(&mut self, ident: &Ident) {
        // ignore quote style
        self.add_operand(Operand::Ident(ident.value.clone()));
    }

    fn visit_compound_ident(&mut self, idents: &[Ident]) {
        let mut ident = String::from(&idents[0].value);

        for part in &idents[1..] {
            write!(ident, ".{}", part.value).unwrap();
        }

        self.add_operand(Operand::Ident(ident))
    }

    fn visit_wildcard(&mut self) {
        self.add_operand(Operand::Wildcard);
    }

    fn visit_table_alias(&mut self, _alias: &TableAlias) {
        self.add_operator(Operator::TableAlias);
    }

    fn visit_object_name(&mut self, name: &ObjectName) {
        // An object name is just a compound identifier in disguise.
        self.visit_compound_ident(&name.0);
    }

    fn visit_join(&mut self, join: &Join) {
        use JoinOperator::*;

        // the JOIN kind and the constraint kind are
        // not explicitly visited by `Join::accept`.
        let (kind, constraint) = match join.join_operator {
            Inner(ref c) => (Operator::InnerJoin, Some(c)),
            LeftOuter(ref c) => (Operator::LeftJoin, Some(c)),
            RightOuter(ref c) => (Operator::RightJoin, Some(c)),
            FullOuter(ref c) => (Operator::OuterJoin, Some(c)),
            CrossJoin => (Operator::CrossJoin, None),
            CrossApply => (Operator::CrossApply, None),
            OuterApply => (Operator::OuterApply, None),
        };

        self.add_operator(kind);

        if let Some(constraint) = constraint {
            let kind = match *constraint {
                JoinConstraint::On(_) => Operator::OnJoinConstraint,
                JoinConstraint::Using(_) => Operator::UsingJoinConstraint,
                JoinConstraint::Natural => Operator::NaturalJoinConstraint,
                JoinConstraint::None => return,
            };

            self.add_operator(kind);
        }
    }

    fn visit_unary_op(&mut self, op: &Expr) {
        let kind = match *op {
            Expr::IsNull(_) => Operator::IsNull,
            Expr::IsNotNull(_) => Operator::IsNotNull,
            Expr::Exists(_) => Operator::Exists,
            Expr::UnaryOp { ref op, expr: _ } => Operator::Unary(op.clone()),
            _ => unreachable!("not a unary operator: `{}`", op),
        };

        self.add_operator(kind);
    }

    fn visit_binary_op(&mut self, op: &Expr) {
        let kind = match *op {
            Expr::InList { .. } | Expr::InSubquery { .. } => Operator::In,
            Expr::Cast { .. } => Operator::Cast,
            Expr::Function(_) => Operator::Function,
            Expr::BinaryOp { ref op, .. } => Operator::Binary(op.clone()),
            _ => unreachable!("not a binary operator: `{}`", op),
        };

        self.add_operator(kind);
    }

    fn visit_between(&mut self, negated: bool) {
        self.add_operator(Operator::Between { negated });
    }

    fn visit_value(&mut self, value: &Value) {
        let operand = match *value {
            Value::Null => Operand::Null,
            Value::Boolean(b) => Operand::Boolean(b),
            Value::Number(ref n, _) => Operand::Number(n.clone()),
            | Value::SingleQuotedString(ref s)
            | Value::DoubleQuotedString(ref s)
            | Value::NationalStringLiteral(ref s)
            | Value::HexStringLiteral(ref s) // maybe handle this differently?
                => Operand::String(s.clone()),
            Value::Interval { .. } => todo!("INTERVAL is not yet supported"),
        };

        self.add_operand(operand);
    }

    fn visit_type(&mut self, _type: &DataType) {
        todo!()
    }

    fn visit_case(
        &mut self,
        _discriminant: Option<&Expr>,
        _conditions: &[Expr],
        _results: &[Expr],
        _else_result: Option<&Expr>,
    ) {
        self.add_operator(Operator::Case);
    }
}

/// Computes all Halstead metrics.
#[derive(Clone, Copy, Debug)]
pub struct HalsteadMetrics {
    /// Program vocabulary, `eta = eta_1 + eta_2`
    pub vocabulary: f64,
    /// Program length, `N = N_1 + N_2`
    pub length: f64,
    /// Calculated estimated program length,
    /// `N_hat = eta_1 * log2(eta_1) + eta_2 * log2(eta_2)`
    pub estimated_length: f64,
    /// Program volume, `V = N * log2(eta)`
    pub volume: f64,
    /// Difficulty of understanding, `D = eta_1 / 2 * N_2 / eta_2`
    pub difficulty: f64,
    /// Implementation effort, `E = D * V`
    pub effort: f64,
}

impl HalsteadMetrics {
    /// Compute Halstead metrics from `eta_1`, `eta_2`, `N_1`, `N_2`
    pub fn new(eta_1: usize, eta_2: usize, n_1: usize, n_2: usize) -> Self {
        let eta_1 = eta_1 as f64;
        let eta_2 = eta_2 as f64;
        let n_1 = n_1 as f64;
        let n_2 = n_2 as f64;

        let eta = eta_1 + eta_2;
        let n = n_1 + n_2;
        let n_hat = eta_1 * f64::log2(eta_1) + eta_2 * f64::log2(eta_2);
        let v = n * f64::log2(eta);
        let d = eta_1 / 2.0 * (n_2 / eta_2);
        let e = d * v;

        HalsteadMetrics {
            vocabulary: eta,
            length: n,
            estimated_length: n_hat,
            volume: v,
            difficulty: d,
            effort: e,
        }
    }
}

/// Returns the total number of nodes in the AST.
pub fn halstead_metrics(stmt: &Statement) -> HalsteadMetrics {
    let mut v = HalsteadVisitor::default();

    stmt.accept(&mut v);

    let eta_1 = v.operators.len();
    let eta_2 = v.operands.len();
    let n_1 = v.operators.values().sum::<usize>();
    let n_2 = v.operands.values().sum::<usize>();

    HalsteadMetrics::new(eta_1, eta_2, n_1, n_2)
}
