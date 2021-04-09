//! Token-based metrics.

use std::collections::HashMap;
use sqlparser::tokenizer::Token;

/// The number of non-whitespace and non-comment tokens in the source.
pub fn token_count<'a>(iter: impl IntoIterator<Item=&'a Token>) -> usize {
    iter.into_iter()
        .filter(|token| match *token {
            Token::EOF | Token::Whitespace(_) => false,
            Token::Char(c) => panic!("non-tokenizable character: `{}`", c),
            _ => true,
        })
        .count()
}

/// String entropy, where the alphabet is based on tokens
/// rather than characters.
pub fn token_entropy<'a>(iter: impl IntoIterator<Item=&'a Token>) -> f64 {
    let tokens = iter.into_iter()
        .filter_map(|token| match *token {
            Token::EOF | Token::Whitespace(_) => None,
            Token::Char(c) => panic!("non-tokenizable character: `{}`", c),
            Token::NationalStringLiteral(ref s) => Some(
                Token::SingleQuotedString(s.clone())
            ),
            _ => Some(token.clone()),
        });

    // Collect absolute frequencies of tokens
    let mut map = HashMap::new();

    for token in tokens {
        *map.entry(token).or_insert(0) += 1;
    }

    // Compute probabilities and entropy
    let n = map.values().sum::<usize>();
    let p = map.values().map(|&k| k as f64 / n as f64);
    let s = -p.map(|p| p * f64::ln(p)).sum::<f64>();

    s
}
