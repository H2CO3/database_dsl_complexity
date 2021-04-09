use sqlparser::ast::*;
use crate::visitor::{ Visitor, Acceptor };

pub use token::{ token_count, token_entropy };
pub use node_count::node_count;
pub use weighted_node_count::weighted_node_count;
pub use halstead::halstead_metrics;

mod token;
mod node_count;
mod weighted_node_count;
mod halstead;

impl Acceptor for Statement {
    fn accept<V: Visitor>(&self, visitor: &mut V) {
        // delegate to individual variants
        match *self {
            Statement::Query(ref query) => query.accept(visitor),
            _ => todo!("unimplemented: {:#?}", self),
        }
    }
}

impl Acceptor for Query {
    fn accept<V: Visitor>(&self, visitor: &mut V) {
        visitor.visit_query(self);

        // visit children
        if let Some(ref with) = self.with {
            with.accept(visitor);
        }

        self.body.accept(visitor);

        for order_by in &self.order_by {
            order_by.expr.accept(visitor);
        }

        if let Some(ref limit) = self.limit {
            limit.accept(visitor);
        }

        if let Some(ref offset) = self.offset {
            offset.value.accept(visitor);
        }

        if let Some(ref _fetch) = self.fetch {
            todo!("FETCH is not yet supported");
        }
    }
}

impl Acceptor for With {
    fn accept<V: Visitor>(&self, visitor: &mut V) {
        visitor.visit_with(self);

        for cte in &self.cte_tables {
            cte.accept(visitor);
        }
    }
}

impl Acceptor for Cte {
    fn accept<V: Visitor>(&self, visitor: &mut V) {
        visitor.visit_cte(self);

        self.alias.accept(visitor);
        self.query.accept(visitor);
    }
}

impl Acceptor for Expr {
    fn accept<V: Visitor>(&self, visitor: &mut V) {
        match *self {
            Expr::Identifier(ref ident) => ident.accept(visitor),
            Expr::QualifiedWildcard(ref name) => {
                let name = ObjectName(name.to_owned());
                name.accept(visitor);
                visitor.visit_wildcard();
            }
            Expr::Wildcard => visitor.visit_wildcard(),
            Expr::CompoundIdentifier(ref idents) => {
                visitor.visit_compound_ident(idents)
            }
            Expr::IsNull(ref expr) | Expr::IsNotNull(ref expr) => {
                visitor.visit_unary_op(self);

                expr.accept(visitor)
            }
            Expr::InList {
                ref expr,
                ref list,
                negated: _
            } => {
                visitor.visit_binary_op(self);
                expr.accept(visitor);

                for item in list {
                    item.accept(visitor);
                }
            }
            Expr::InSubquery {
                ref expr,
                ref subquery,
                negated: _,
            } => {
                visitor.visit_binary_op(self);
                expr.accept(visitor);
                subquery.accept(visitor);
            }
            Expr::Exists(ref query) => {
                visitor.visit_unary_op(self);
                query.accept(visitor);
            }
            Expr::Subquery(ref query) => query.accept(visitor),
            Expr::Between {
                ref expr,
                negated,
                ref low,
                ref high,
            } => {
                visitor.visit_between(negated);
                expr.accept(visitor);
                low.accept(visitor);
                high.accept(visitor);
            }
            Expr::BinaryOp {
                ref left,
                op: _,
                ref right,
            } => {
                visitor.visit_binary_op(self);
                left.accept(visitor);
                right.accept(visitor);
            }
            Expr::UnaryOp { op: _, ref expr } => {
                visitor.visit_unary_op(self);
                expr.accept(visitor);
            }
            // Do not count parentheses separately
            Expr::Nested(ref inner) => inner.accept(visitor),
            Expr::Value(ref value) => value.accept(visitor),
            Expr::ListAgg(_) => todo!("LISTAGG is not yet supported"),
            Expr::Extract { .. } => todo!("EXTRACT is not yet supported"),
            Expr::Collate { .. } => todo!("COLLATE is not yet supported"),
            Expr::TypedString { .. } => todo!("Typed strings are not yet supported"),
            Expr::Cast { ref expr, ref data_type } => {
                visitor.visit_binary_op(self);

                expr.accept(visitor);
                data_type.accept(visitor);
            }
            Expr::Case {
                ref operand,
                ref conditions,
                ref results,
                ref else_result,
            } => {
                assert_eq!(conditions.len(), results.len());

                visitor.visit_case(operand.as_deref(),
                                   conditions,
                                   results,
                                   else_result.as_deref());

                if let Some(ref discriminant) = *operand {
                    discriminant.accept(visitor);
                }

                for (cond, res) in conditions.iter().zip(results) {
                    cond.accept(visitor);
                    res.accept(visitor);
                }

                if let Some(ref else_result) = *else_result {
                    else_result.accept(visitor);
                }
            }
            Expr::Function(ref func) => {
                // a function is a binary operation:
                // LHS is the name, RHS is the argument list
                visitor.visit_binary_op(self);
                func.accept(visitor);
            }
            Expr::Substring {
                ref expr,
                ref substring_from,
                ref substring_for,
            } => {
                // `SUBSTRING` is just a function call,
                // so treat it identically.
                visitor.visit_binary_op(self);
                expr.accept(visitor);

                if let Some(arg) = substring_from.as_deref() {
                    arg.accept(visitor);
                }
                if let Some(arg) = substring_for.as_deref() {
                    arg.accept(visitor);
                }
            }
            Expr::MapAccess { ref column, ref key } => {
                visitor.visit_binary_op(self);
                column.accept(visitor);

                let key_str = Value::SingleQuotedString(key.clone());
                key_str.accept(visitor);
            }
        }
    }
}

impl Acceptor for Value {
    fn accept<V: Visitor>(&self, visitor: &mut V) {
        visitor.visit_value(self)
    }
}

impl Acceptor for Function {
    fn accept<V: Visitor>(&self, visitor: &mut V) {
        self.name.accept(visitor);

        for arg in &self.args {
            arg.accept(visitor);
        }

        if let Some(ref _over) = self.over {
            todo!("OVER(...) window spec is not yet supported");
        }
    }
}

impl Acceptor for DataType {
    fn accept<V: Visitor>(&self, visitor: &mut V) {
        visitor.visit_type(self);

        // the only recursive type is `Array`
        if let DataType::Array(ref item) = *self {
            item.accept(visitor)
        }
    }
}

impl Acceptor for SetExpr {
    fn accept<V: Visitor>(&self, visitor: &mut V) {
        // delegate to newtypes, but visit if
        // child is not a newtype (`SetOperation`)
        match *self {
            SetExpr::Select(ref select) => select.accept(visitor),
            SetExpr::Query(ref query) => query.accept(visitor),
            SetExpr::SetOperation {
                ref op, all, ref left, ref right // `SetOperator` is !Copy
            } => {
                visitor.visit_set_op(op.clone(), all, left, right);
                left.accept(visitor);
                right.accept(visitor);
            }
            SetExpr::Values(ref values) => values.accept(visitor),
            SetExpr::Insert(_) => todo!("Unsupported set operation: {:#?}", self),
        }
    }
}

impl Acceptor for Values {
    fn accept<V: Visitor>(&self, visitor: &mut V) {
        visitor.visit_values(self);

        for row in &self.0 {
            for field in row {
                field.accept(visitor);
            }
        }
    }
}

impl Acceptor for OrderByExpr {
    fn accept<V: Visitor>(&self, visitor: &mut V) {
        self.expr.accept(visitor)
    }
}

impl Acceptor for Select {
    fn accept<V: Visitor>(&self, visitor: &mut V) {
        visitor.visit_select(self);

        // visit children
        if let Some(ref _top) = self.top {
            todo!("SELECT TOP ... is not yet supported");
        }

        for projection in &self.projection {
            projection.accept(visitor);
        }

        for from in &self.from {
            from.accept(visitor);
        }

        if let Some(ref selection) = self.selection {
            selection.accept(visitor);
        }

        for group_by in &self.group_by {
            group_by.accept(visitor);
        }

        if let Some(ref having) = self.having {
            having.accept(visitor);
        }
    }
}

impl Acceptor for SelectItem {
    fn accept<V: Visitor>(&self, visitor: &mut V) {
        match *self {
            SelectItem::UnnamedExpr(ref expr) => expr.accept(visitor),
            SelectItem::ExprWithAlias {
                ref expr,
                ref alias,
            } => {
                expr.accept(visitor);
                alias.accept(visitor);
            }
            SelectItem::QualifiedWildcard(ref name) => {
                name.accept(visitor);
                visitor.visit_wildcard();
            }
            SelectItem::Wildcard => visitor.visit_wildcard(),
        }
    }
}

impl Acceptor for Ident {
    fn accept<V: Visitor>(&self, visitor: &mut V) {
        visitor.visit_ident(self)
    }
}

impl Acceptor for TableWithJoins {
    fn accept<V: Visitor>(&self, visitor: &mut V) {
        // this is not counted as a single node, because it
        // represents either a single table, or a table and joins,
        // and a single table shouldn't be counted as two nodes, so
        // we just delegate to the inner `relations: TableFactor`.
        self.relation.accept(visitor);

        for join in &self.joins {
            join.accept(visitor);
        }
    }
}

impl Acceptor for TableFactor {
    fn accept<V: Visitor>(&self, visitor: &mut V) {
        match *self {
            TableFactor::Table {
                ref name,
                ref alias,
                ref args,
                ref with_hints,
            } => {
                name.accept(visitor);

                if let Some(ref alias) = *alias {
                    alias.accept(visitor);
                }

                if with_hints.len() > 0 {
                    todo!("WITH hints are not yet supported");
                }

                for arg in args {
                    arg.accept(visitor);
                }

            }
            TableFactor::Derived {
                lateral: _,
                ref subquery,
                ref alias,
            } => {
                subquery.accept(visitor);

                if let Some(ref alias) = *alias {
                    alias.accept(visitor);
                }
            }
            TableFactor::TableFunction {
                ref expr,
                ref alias,
            } => {
                expr.accept(visitor);

                if let Some(ref alias) = *alias {
                    alias.accept(visitor);
                }
            }
            TableFactor::NestedJoin(ref inner) => inner.accept(visitor)
        }
    }
}

impl Acceptor for ObjectName {
    fn accept<V: Visitor>(&self, visitor: &mut V) {
        visitor.visit_object_name(self);
    }
}

impl Acceptor for FunctionArg {
    fn accept<V: Visitor>(&self, visitor: &mut V) {
        use FunctionArg::*;

        match *self {
            Unnamed(ref arg) | Named { ref arg, name: _ } => {
                arg.accept(visitor)
            }
        }
    }
}

impl Acceptor for TableAlias {
    fn accept<V: Visitor>(&self, visitor: &mut V) {
        visitor.visit_table_alias(self);

        self.name.accept(visitor);

        for column in &self.columns {
            column.accept(visitor);
        }
    }
}

impl Acceptor for Join {
    fn accept<V: Visitor>(&self, visitor: &mut V) {
        visitor.visit_join(self);

        self.relation.accept(visitor);
        self.join_operator.accept(visitor);
    }
}

impl Acceptor for JoinOperator {
    fn accept<V: Visitor>(&self, visitor: &mut V) {
        use JoinOperator::*;

        match *self {
            Inner(ref cond) |
            LeftOuter(ref cond) |
            RightOuter(ref cond) |
            FullOuter(ref cond) => cond.accept(visitor),
            CrossJoin | CrossApply | OuterApply => {}
        }
    }
}

impl Acceptor for JoinConstraint {
    fn accept<V: Visitor>(&self, visitor: &mut V) {
        match *self {
            JoinConstraint::On(ref expr) => expr.accept(visitor),
            JoinConstraint::Using(ref idents) => {
                for ident in idents {
                    ident.accept(visitor);
                }
            }
            JoinConstraint::Natural | JoinConstraint::None => {}
        }
    }
}
