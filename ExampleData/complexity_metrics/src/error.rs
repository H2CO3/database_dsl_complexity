use std::io;
use sqlparser::{
    tokenizer::TokenizerError,
    parser::ParserError,
};
use thiserror::Error;


#[derive(Debug, Error)]
pub enum SqlError {
    #[error(transparent)]
    IO(#[from] io::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error("tokenizer error at line {} char {}: {}", .0.line, .0.col, .0.message)]
    Tokenizer(TokenizerError),
    #[error(transparent)]
    Parser(#[from] ParserError),
    #[error("unrecognized dialect: `{0}`")]
    Dialect(String),
    #[error("SQLite error: {0}")]
    SQLite(#[from] rusqlite::Error),
}

impl From<TokenizerError> for SqlError {
    fn from(error: TokenizerError) -> Self {
        SqlError::Tokenizer(error)
    }
}
