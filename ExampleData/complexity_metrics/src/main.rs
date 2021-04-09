use std::io::{ stdin, Read, Result as IoResult };
use structopt::StructOpt;
use serde::Serialize;
use serde_json::{ json, Value };
use sqlparser::{
    tokenizer::{ Tokenizer, Token, TokenizerError },
    parser::{ Parser, ParserError },
    ast::Statement,
};
use error::SqlError;
use dialect::SqlDialect;

mod error;
mod dialect;
mod visitor;
mod histogram;
mod recursive;
mod metrics;

/// Compute complexity metrics for the given SQL string.
#[derive(Debug, Clone, StructOpt)]
enum Command {
    /// Compute complexity metrics for the SQL code read from `stdin`.
    Complexity {
        /// The SQL dialect to adhere to when parsing the input.
        #[structopt(short = "d", long = "dialect", default_value = "generic")]
        dialect: SqlDialect,
    },
    /// Compute a histogram of token kinds using SQL queries in the
    /// appropriately-formatted SQLite database, containing samples
    /// from the Stack Exchange Data Explorer.
    Histogram {
        /// File name of the SQLite database containing the queries.
        /// If `-`, queries are read from standard input instead of a DB.
        #[structopt(short = "i", long = "input", default_value = "sede_stackoverflow_favorites.sqlite3")]
        input: String,
        /// The SQL dialect to adhere to when parsing the input.
        #[structopt(short = "d", long = "dialect", default_value = "mssql")]
        dialect: SqlDialect,
    },
    /// Searches for potentially recursive queries, i.e. where
    /// the name of a CTE is referenced in its own definition.
    Recursive {
        /// File name of the SQLite database containing the queries.
        #[structopt(short = "i", long = "input", default_value = "sede_stackoverflow_favorites.sqlite3")]
        input: String,
        /// The SQL dialect to adhere to when parsing the input.
        #[structopt(short = "d", long = "dialect", default_value = "mssql")]
        dialect: SqlDialect,
    }
}

impl Command {
    fn run(self) -> Result<(), SqlError> {
        match self {
            Command::Complexity { dialect } => Self::compute_complexity(dialect),
            Command::Histogram {
                input,
                dialect,
            } => Self::compute_histogram(input, dialect),

            Command::Recursive {
                input,
                dialect,
            } => Self::search_recursive(input, dialect),
        }
    }

    fn compute_complexity(dialect: SqlDialect) -> Result<(), SqlError> {
        let sql = read_preprocess_sql()?;
        let tokens = tokenize(&sql, dialect)?;

        // Token-based metrics
        let token_count = metrics::token_count(&tokens);
        let token_entropy = metrics::token_entropy(&tokens);

        // AST Node-based metrics
        let stmt = parse(tokens, dialect)?;
        let typed_node_count = metrics::node_count(&stmt);
        let weighted_node_count = metrics::weighted_node_count(&stmt);

        let v = serde_json::to_value(&stmt)?;
        let v = thin_value(v);
        let json_node_count = json_node_count(&v);

        // Halstead complexity metrics
        let halstead = metrics::halstead_metrics(&stmt);

        // Print results in JSON
        let value = json!({
            "token_count": token_count,
            "token_entropy": token_entropy,
            "node_count": typed_node_count,
            "weighted_node_count": weighted_node_count,
            // "json_node_count": json_node_count,
            "halstead_vocabulary": halstead.vocabulary,
            "halstead_length": halstead.length,
            "halstead_estimated_length": halstead.estimated_length,
            "halstead_volume": halstead.volume,
            "halstead_difficulty": halstead.difficulty,
            "halstead_effort": halstead.effort,
        });

        json_dump(&value)?;

        Ok(())
    }

    fn compute_histogram(filename: String, dialect: SqlDialect) -> Result<(), SqlError> {
        let histogram = if filename == "-" {
            let mut code = String::new();
            std::io::stdin().read_to_string(&mut code)?;
            histogram::token_histogram_str(&code, dialect)?
        } else {
            let conn = rusqlite::Connection::open(&filename)?;
            histogram::token_histogram_db(&conn, dialect)?
        };

        json_dump(&histogram)?;

        Ok(())
    }

    fn search_recursive(filename: String, dialect: SqlDialect) -> Result<(), SqlError> {
        let conn = rusqlite::Connection::open(&filename)?;
        let queries = recursive::recursive_queries(&conn, dialect)?;

        json_dump(&queries)?;

        Ok(())
    }
}

/// Dump a serializable value as JSON to the standard output stream.
fn json_dump<T: Serialize>(value: &T) -> serde_json::Result<()> {
    serde_json::to_writer_pretty(std::io::stdout(), value)
}

/// Read SQL from standard input and replace
/// argument placeholders with literal zero.
fn read_preprocess_sql() -> IoResult<String> {
    let mut buf = String::new();
    stdin().read_to_string(&mut buf)?;

    // Replace argument placeholders with literal zeroes.
    // This doesn't even affect the character count, so
    // it won't affect any complexity metrics, either.
    // (The resulting SQL statement might not be correct
    // with regards to types, but we are not trying to
    // typecheck the source here, so that is fine.)
    //
    // This also replaces *every* question mark with the
    // character '0', but our queries only use question
    // marks for placeholders (and even inside string
    // literals, this would not change the complexity
    // of the query either, so this is fine as well.)
    let processed = buf.replace('?', "0");

    Ok(processed)
}

/// Split a SQL statement into tokens.
fn tokenize(sql: &str, dialect: SqlDialect) -> Result<Vec<Token>, TokenizerError> {
    Tokenizer::new(&dialect, &sql).tokenize()
}

/// Parse a SQL statement. Only a single statement is supported.
fn parse(tokens: Vec<Token>, dialect: SqlDialect) -> Result<Statement, SqlError> {
    let mut parser = Parser::new(tokens, &dialect);
    let stmt = parser.parse_statement()?;

    // skip trailing semicolon, if any
    let _ = parser.consume_token(&Token::SemiColon);

    if parser.peek_token() == Token::EOF {
        Ok(stmt)
    } else {
        println!("{:?}", parser.peek_token());
        Err(ParserError::ParserError("got multiple statements".into()).into())
    }
}

/// Replace externally-tagged enums with their inner value
/// in order to remove one extra, effectively artefactual
/// level of nesting, which does not relate to the inherent
/// depth of the AST node but to its JSON representation.
fn thin_value(value: Value) -> Value {
    match value {
        Value::Object(map) => {
            // Replace enum with its value
            if map.len() == 1 {
                let key = map.keys().next().unwrap();
                if let Some(ch) = key.chars().next() {
                    if ch.is_ascii_uppercase() {
                        let inner = map.into_iter().next().unwrap().1;
                        return thin_value(inner);
                    }
                }
            }

            // remove nulls and empty arrays
            map.into_iter()
                .filter(|&(_, ref v)| {
                    match *v {
                        Value::Null => false,
                        Value::Array(ref vec) => vec.len() > 0,
                        _ => true,
                    }
                })
                .map(|(k, v)| (k, thin_value(v)))
                .collect()
        },
        Value::Array(vec) => vec.into_iter().map(thin_value).collect(),
        _ => value,
    }
}

/// Count the total number of nodes in a JSON `Value` tree.
fn json_node_count(value: &Value) -> usize {
    match *value {
        Value::Array(ref arr) => {
            arr.iter().map(json_node_count).sum::<usize>() + 1
        }
        Value::Object(ref object) => {
            // keys are all strings, so we just count entries
            object.values().map(json_node_count).sum::<usize>() + 1
        }
        _ => 1 // scalars
    }
}

fn main() -> Result<(), SqlError> {
    Command::from_args().run()
}
