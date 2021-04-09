use std::str::FromStr;
use sqlparser::dialect::{
    Dialect,
    AnsiDialect,
    GenericDialect,
    MsSqlDialect,
    MySqlDialect,
    PostgreSqlDialect,
    SQLiteDialect,
    SnowflakeDialect,
};
use crate::error::SqlError;

/// Enum for moving the choice of SQL dialect
/// from compile-time to runtime.
#[derive(Clone, Copy, Debug)]
pub enum SqlDialect {
    Ansi,
    Generic,
    MsSql,
    MySql,
    PostgreSql,
    SQLite,
    Snowflake,
}

impl FromStr for SqlDialect {
    type Err = SqlError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        use SqlDialect::*;

        Ok(match s {
            "ansi"       => Ansi,
            "generic"    => Generic,
            "mssql"      => MsSql,
            "mysql"      => MySql,
            "postgresql" => PostgreSql,
            "sqlite"     => SQLite,
            "snowflake"  => Snowflake,
            _ => return Err(SqlError::Dialect(s.to_owned()))
        })
    }
}

impl Dialect for SqlDialect {
    fn is_identifier_start(&self, ch: char) -> bool {
        use SqlDialect::*;

        match *self {
            Ansi       => AnsiDialect{}.is_identifier_start(ch),
            Generic    => GenericDialect.is_identifier_start(ch),
            MsSql      => MsSqlDialect{}.is_identifier_start(ch),
            MySql      => MySqlDialect{}.is_identifier_start(ch),
            PostgreSql => PostgreSqlDialect{}.is_identifier_start(ch),
            SQLite     => SQLiteDialect{}.is_identifier_start(ch),
            Snowflake  => SnowflakeDialect.is_identifier_start(ch),
        }
    }

    fn is_identifier_part(&self, ch: char) -> bool {
        use SqlDialect::*;

        match *self {
            Ansi       => AnsiDialect{}.is_identifier_part(ch),
            Generic    => GenericDialect.is_identifier_part(ch),
            MsSql      => MsSqlDialect{}.is_identifier_part(ch),
            MySql      => MySqlDialect{}.is_identifier_part(ch),
            PostgreSql => PostgreSqlDialect{}.is_identifier_part(ch),
            SQLite     => SQLiteDialect{}.is_identifier_part(ch),
            Snowflake  => SnowflakeDialect.is_identifier_part(ch),
        }
    }

    fn is_delimited_identifier_start(&self, ch: char) -> bool {
        use SqlDialect::*;

        match *self {
            Ansi       => AnsiDialect{}.is_delimited_identifier_start(ch),
            Generic    => GenericDialect.is_delimited_identifier_start(ch),
            MsSql      => MsSqlDialect{}.is_delimited_identifier_start(ch),
            MySql      => MySqlDialect{}.is_delimited_identifier_start(ch),
            PostgreSql => PostgreSqlDialect{}.is_delimited_identifier_start(ch),
            SQLite     => SQLiteDialect{}.is_delimited_identifier_start(ch),
            Snowflake  => SnowflakeDialect.is_delimited_identifier_start(ch),
        }
    }
}
