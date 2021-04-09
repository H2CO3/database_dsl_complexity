//! This module counts the frequency of each kind of token
//! in the corpus derived from the Stack Exchange Data Explorer.

use std::cmp::{ PartialOrd, Ord, Ordering };
use std::collections::HashMap;
use rusqlite::{ params, Connection };
use sqlparser::{
    tokenizer::{ Tokenizer, Token },
    dialect::keywords::Keyword,
};
use serde::ser::{ Serialize, Serializer, SerializeStruct };
use crate::{
    dialect::SqlDialect,
    error::SqlError,
};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TokenFrequency {
    /// The normalized token.
    pub token: Token,
    /// The count of this kind of token in the input.
    pub frequency: usize,
}

/// Orders by _decreasing_ order of frequency first,
/// then lexicographically by the stringified token.
impl Ord for TokenFrequency {
    fn cmp(&self, other: &Self) -> Ordering {
        match other.frequency.cmp(&self.frequency) {
            Ordering::Equal => {
                let lhs = self.token.to_string();
                let rhs = other.token.to_string();

                lhs.cmp(&rhs)
            }
            // Avoid calling `to_string()` when the frequencies differ.
            ord @ _ => ord
        }
    }
}

impl PartialOrd for TokenFrequency {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.cmp(other).into()
    }
}

impl Serialize for TokenFrequency {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut s = serializer.serialize_struct("TokenFrequency", 2)?;
        s.serialize_field("token", &format_args!("{}", self.token))?;
        s.serialize_field("frequency", &self.frequency)?;
        s.end()
    }
}

/// Counts each kind of token in all of the queries in the specified string.
/// Increments the frequency of each kind of token in the `histogram` map.
/// Different keywords count as different tokens, because they potentially
/// correspond to concepts and operations of different levels of difficulty.
/// In contrast, literals (string and number) and identifiers are considered
/// to be of the same kind, because the concrete contents of a string literal
/// or an identifier do not influence its conceptial difficulty.
fn update_histogram(
    sql: &str,
    dialect: SqlDialect,
    histogram: &mut HashMap<Token, usize>
) -> Result<(), SqlError> {
    let tokens = Tokenizer::new(&dialect, sql).tokenize()?;

    let iter = tokens
        .into_iter()
        .filter_map(|token| match token {
            Token::EOF | Token::Whitespace(_) => None,

            Token::Char(_) => None, // ignore unparseable

            Token::Number(_, _) => Some(Token::Number(String::from("0"), false)),

            | Token::SingleQuotedString(_)
            | Token::NationalStringLiteral(_)
            | Token::HexStringLiteral(_) => Some(
                Token::SingleQuotedString(String::from("string literal"))
            ),

            Token::Word(mut word) => {
                word.quote_style = None; // ignore quoting

                // all non-keyword words are assumed to
                // be of the same kind: an identifier.
                // Keywords are converted to ALL CAPS,
                // because they are case insensitive.
                if word.keyword == Keyword::NoKeyword {
                    word.value = String::from("identifier");
                } else {
                    word.value = word.value.to_uppercase();
                }

                Some(Token::Word(word))
            }

            _ => Some(token)
        });

    for token in iter {
        *histogram.entry(token).or_insert(0) += 1;
    }

    Ok(())
}

/// Creates a sorted histogram from an unordered `HashMap`.
fn sorted_histogram(map: HashMap<Token, usize>) -> Vec<TokenFrequency> {
    let mut vec: Vec<_> = map
        .into_iter()
        .map(|(token, frequency)| TokenFrequency { token, frequency })
        .collect();

    vec.sort();

    vec
}

/// Counts each kind of token in all of the queries in the specified database.
pub fn token_histogram_db(conn: &Connection, dialect: SqlDialect)
    -> Result<Vec<TokenFrequency>, SqlError>
{
    let mut map = HashMap::new();
    let mut stmt = conn.prepare("SELECT id, code FROM snippet")?;
    let mut rows = stmt.query(params![])?;

    while let Some(row) = rows.next()? {
        let id: i64 = row.get(0)?;
        let code: String = row.get(1)?;

        match update_histogram(&code, dialect, &mut map) {
            Ok(()) => {}
            Err(err) => {
                eprintln!("Can't tokenize query #{}; ignoring ({})", id, SqlError::from(err));
                continue;
            }
        };
    }

    Ok(sorted_histogram(map))
}

/// Counts each kind of token in all of the queries in the specified string.
pub fn token_histogram_str(sql: &str, dialect: SqlDialect)
    -> Result<Vec<TokenFrequency>, SqlError>
{
    let mut map = HashMap::new();

    update_histogram(sql, dialect, &mut map)?;

    Ok(sorted_histogram(map))
}
