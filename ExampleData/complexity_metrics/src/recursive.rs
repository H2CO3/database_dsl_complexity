//! Finds potentially recursive queries.

use once_cell::sync::Lazy;
use regex::Regex;
use serde::{ Serialize, Deserialize };
use sqlparser::{
    parser::Parser,
    ast::*
};
use rusqlite::{ params, Connection };
use crate::{
    dialect::SqlDialect,
    error::SqlError,
};

/// Metadata for a query and the SQL source text itself.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryDesc {
    /// Unique ID in Stack Exchange Data Explorer (`query/XXX`).
    pub id: i64,
    /// The SQL source text.
    pub code: String,
    /// Total number of occurrences of recursion
    /// in all statements of this query.
    pub recursion_count: usize,
}

/// Finds potentially recursive SQL queries.
/// CTEs are marked as recursive if either of the following criteria is met:
///
/// * The CTE is explicitly marked `RECURSIVE`.
/// * The CTE references its own name or alias in its own definition.
///
/// These heuristics are not perfect, but the goal is only to provide
/// an approximate frequency of the use of recursion, and to prune
/// the search space of 7000+ queries so that it becomes possible to
/// be processed by a human reader in a reasonable amount of time.
pub fn recursive_queries(conn: &Connection, dialect: SqlDialect) -> Result<Vec<QueryDesc>, SqlError> {
    let mut stmt = conn.prepare(r"
        SELECT id, code
        FROM snippet
        WHERE code LIKE ?
    ")?;
    let mut rows = stmt.query(params!["%WITH%"])?;
    let mut qs = Vec::new();

    while let Some(row) = rows.next()? {
        let mut q = QueryDesc {
            id: row.get(0)?,
            code: row.get(1)?,
            recursion_count: 0,
        };

        // Check if the code contains a `maxrecursion` setting.
        // The AST-walking heuristics don't seem to be able to
        // find some of the recursive queries, but nonzero
        // values of this setting are a strong hint that the
        // query is in fact recursive.
        if max_recursion_limit(&q.code) > 0 {
            q.recursion_count = 1;
            qs.push(q);
            continue;
        }

        let stmts = match Parser::parse_sql(&dialect, &q.code) {
            Ok(stmts) => stmts,
            Err(err) => {
                // Just ignore erroneous queries
                eprintln!("Can't parse query #{}; ignoring ({})", q.id, err);
                continue;
            }
        };

        for stmt in stmts {
            q.recursion_count += count_recursion(&stmt);
        }

        // Only add to the result set the queries that
        // actually seem to contain some recursion.
        if q.recursion_count > 0 {
            qs.push(q);
        }
    }

    Ok(qs)
}

/// Actually counts the number of recursive constructs
/// in the specified SQL statement.
fn count_recursion(stmt: &Statement) -> usize {
    match *stmt {
        Statement::Query(ref query) => count_recursion_query(&**query, Vec::new()),
        Statement::CreateTable { query: None, .. } => 0,
        Statement::CreateTable {
            query: Some(ref query),
            ..
        } => count_recursion_query(&**query, Vec::new()),
        Statement::Insert {
            ref source,
            ..
        } => count_recursion_query(&**source, Vec::new()),
        Statement::Delete { selection: None, .. } => 0,
        Statement::Delete {
            selection: Some(ref expr),
            ..
        } => count_recursion_expr(expr, &[]),
        _ => todo!("Unsupported statement: {:#?}", stmt),
    }
}

fn count_recursion_query(query: &Query, mut ctes: Vec<String>) -> usize {
    // We accumulate the number of recursive clauses used
    // in this variable.
    let mut total_rec: usize = 0;

    if let Some(ref with) = query.with {
        // Inspect the tables for recursion. Every table can
        // only be recursive within its own definition, so add
        // and remove CTE aliases one-by-one.
        // Ignore identifier case and quoting, just like SQL does.
        for cte in &with.cte_tables {
            ctes.push(cte.alias.name.value.to_lowercase());
            total_rec += count_recursion_query(&cte.query, ctes.clone());
            ctes.pop();
        }
    }

    // Now, check the rest of the query, for any uses
    // of identifiers found in the stack for the AST
    // node one level above.
    total_rec += count_recursion_set_expr(&query.body, &ctes);

    // TODO(H2CO3): it is unlikely that the WHERE, ORDER BY,
    // GROUP BY, HAVING, LIMIT, OFFSET, FETCH, TOP, etc. clauses
    // of an SQL statement would contain recursive references to
    // outer CTEs (is that even valid SQL?). However, we _could_
    // check that as well. But should we?

    // If the query is marked as `RECURSIVE` but our heuristics
    // couldn't find anything actually recursive, then assume
    // 1 occurrence of recursion, for the lack of a better estimate.
    let explicit_recursive = query.with
        .as_ref()
        .map(|w| usize::from(w.recursive))
        .unwrap_or(0);

    std::cmp::max(total_rec, explicit_recursive)
}

fn count_recursion_set_expr(expr: &SetExpr, ctes: &[String]) -> usize {
    match *expr {
        SetExpr::Values(Values(ref values)) => {
            values
                .iter()
                .flat_map(
                    |row| row.iter().map(
                        |col| count_recursion_expr(col, ctes)
                    )
                )
                .sum()

        }
        SetExpr::SetOperation { ref left, ref right, .. } => {
            count_recursion_set_expr(&**left, ctes) + count_recursion_set_expr(&**right, ctes)
        }
        SetExpr::Query(ref query) => count_recursion_query(&**query, ctes.to_owned()),
        SetExpr::Select(ref select) => {
            // The primary sources of recursion are projections and joins.
            let rec_proj: usize = select.projection
                .iter()
                .map(|proj| count_recursion_projection(proj, ctes))
                .sum();

            let rec_from: usize = select.from
                .iter()
                .map(|table| count_recursion_from(table, ctes))
                .sum();

            // TODO(H2CO3): should we be accounting for potential
            // recursion in the WHERE, GROUP BY, HAVING, etc.
            // clauses (`selection`, `group_by`, and `having`
            // fields, respectively)?
            rec_proj + rec_from
        }
        SetExpr::Insert(ref stmt) => count_recursion(stmt),
    }
}

fn count_recursion_projection(item: &SelectItem, ctes: &[String]) -> usize {
    match *item {
        SelectItem::UnnamedExpr(ref expr) => count_recursion_expr(expr, ctes),
        SelectItem::ExprWithAlias { ref expr, alias: _ } => {
            // TODO(H2CO3): should we account for the situation
            // where `alias` shadows the name of a CTE?
            count_recursion_expr(expr, ctes)
        }
        SelectItem::QualifiedWildcard(ref name) => {
            // See if this is a (fully-qualified) reference
            // to a table in the stack of CTEs currently in scope.
            let norm_name = object_name(name);

            usize::from(ctes.contains(&norm_name)) // 0 or 1
        }
        SelectItem::Wildcard => 0, // nothing to see here
    }
}

fn count_recursion_from(table: &TableWithJoins, ctes: &[String]) -> usize {
    let rec_rel = count_recursion_relation(&table.relation, ctes);
    let rec_join: usize = table.joins
        .iter()
        .map(|join| {
            // Intentionally ignore `join.join_operator` and the ON clause:
            // the condition of the ON clause refers to the same table as
            // the projection, so we should not count these as duplicates.
            count_recursion_relation(&join.relation, ctes)
        })
        .sum();

    rec_rel + rec_join
}

fn count_recursion_relation(relation: &TableFactor, ctes: &[String]) -> usize {
    match relation {
        // Ignore `alias`, `args`, and `hints`.
        // TODO(H2CO3): should we handle the alias shadowing a CTE here?
        TableFactor::Table { ref name, .. } => {
            let norm_name = object_name(name);

            usize::from(ctes.contains(&norm_name)) // 0 or 1
        }
        TableFactor::Derived { ref subquery, .. } => {
            count_recursion_query(subquery, ctes.to_owned())
        }
        TableFactor::TableFunction { ref expr, .. } => {
            count_recursion_expr(expr, ctes)
        }
        TableFactor::NestedJoin(ref joins) => count_recursion_from(&**joins, ctes),
    }
}

fn count_recursion_expr(expr: &Expr, ctes: &[String]) -> usize {
    match *expr {
        Expr::Wildcard => 0,
        Expr::Identifier(ref ident) => {
            let name = ident.value.to_lowercase();

            usize::from(ctes.contains(&name))
        }
        Expr::CompoundIdentifier(ref idents) | Expr::QualifiedWildcard(ref idents) => {
            let norm_first = idents[0].value.to_lowercase();
            let norm_name = idents
                .iter()
                .map(|id| id.value.to_lowercase())
                .collect::<Vec<_>>()
                .join(".");

            // check the first part of the name separately,
            // maybe it's the table name itself
            usize::from(ctes.contains(&norm_name) || ctes.contains(&norm_first))
        }
        Expr::IsNull(ref expr) | Expr::IsNotNull(ref expr) => {
            count_recursion_expr(&**expr, ctes)
        }
        Expr::InList { ref expr, ref list, .. } => {
            let rec_expr = count_recursion_expr(&**expr, ctes);
            let rec_list = list
                .iter()
                .map(|item| count_recursion_expr(item, ctes))
                .sum::<usize>();

            rec_expr + rec_list
        }
        Expr::InSubquery { ref expr, ref subquery, .. } => {
            let rec_expr = count_recursion_expr(&**expr, ctes);
            let rec_query = count_recursion_query(&**subquery, ctes.to_owned());

            rec_expr + rec_query
        }
        Expr::Between {
            ref expr,
            ref low,
            ref high,
            ..
        } => {
            [expr, low, high]
                .iter()
                .map(|e| count_recursion_expr(&**e, ctes))
                .sum()
        }
        Expr::BinaryOp { ref left, ref right, .. } => {
            count_recursion_expr(&**left, ctes) + count_recursion_expr(&**right, ctes)
        }
        | Expr::UnaryOp { ref expr, .. }
        | Expr::Cast { ref expr, .. }
        | Expr::Collate { ref expr, .. }
        | Expr::Nested(ref expr) => {
            count_recursion_expr(&**expr, ctes)
        }
        Expr::Exists(ref query) | Expr::Subquery(ref query) => {
            count_recursion_query(query, ctes.to_owned())
        }
        Expr::Value(_) | Expr::TypedString { .. } => 0,
        Expr::ListAgg { .. } => todo!("Unsuppored expression: {:#?}", expr),
        Expr::Function(Function { ref args, .. }) => {
            args.iter()
                .map(|arg| match *arg {
                    | FunctionArg::Named { ref arg, .. }
                    | FunctionArg::Unnamed(ref arg) => {
                        count_recursion_expr(arg, ctes)
                    }
                })
                .sum()
        }
        Expr::Case {
            ref operand,
            ref conditions,
            ref results,
            ref else_result,
        } => {
            let rec_op = match operand.as_deref() {
                Some(expr) => count_recursion_expr(expr, ctes),
                None => 0,
            };
            let rec_conds: usize = conditions
                .iter()
                .map(|e| count_recursion_expr(e, ctes))
                .sum();
            let rec_res: usize = results
                .iter()
                .map(|e| count_recursion_expr(e, ctes))
                .sum();
            let rec_else = match else_result.as_deref() {
                Some(expr) => count_recursion_expr(expr, ctes),
                None => 0,
            };

            rec_op + rec_conds + rec_res + rec_else
        }
        Expr::Substring {
            ref expr,
            ref substring_from,
            ref substring_for,
        } => {
            let rec_expr = count_recursion_expr(&**expr, ctes);
            let rec_from = substring_from.as_deref().map_or(
                0,
                |expr| count_recursion_expr(expr, ctes)
            );
            let rec_for = substring_for.as_deref().map_or(
                0,
                |expr| count_recursion_expr(expr, ctes)
            );

            rec_expr + rec_from + rec_for
        }
        Expr::MapAccess { ref column, .. } => count_recursion_expr(&**column, ctes),
        Expr::Extract { .. } => todo!("Unsupported expression: {:#?}", expr)
    }
}

/// Helper for turning an object name into a normalized name:
///
/// * Stripped of all quoting
/// * Fully lowercase
///
/// This will only be used for equality comparison, not for
/// human-readable display (the result might not even parse
/// correctly as SQL because of the lack of quoting, in fact).
fn object_name(&ObjectName(ref names): &ObjectName) -> String {
    names
        .iter()
        .map(|id| id.value.to_lowercase())
        .collect::<Vec<_>>()
        .join(".")
}

/// Helper for finding a non-zero `maxrecursion` hint
/// in the given piece of SQL.
fn max_recursion_limit(sql: &str) -> usize {
    static REGEX: Lazy<Regex> = Lazy::new(|| {
        Regex::new(r"(?i)maxrecursion\s+(?P<limit>\d+)").unwrap()
    });

    REGEX.captures(sql).map_or(0, |captures| {
        captures["limit"].parse().expect("invalid recursion limit")
    })
}
