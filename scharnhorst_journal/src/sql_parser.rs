//! SQL parser for DebugWriteJournal.
//!
//! This module provides a simple handwritten SQL parser that supports
//! basic UPDATE, INSERT, and DELETE statements for debug builds.
//!
//! Supported syntax:
//! - UPDATE table SET col = val WHERE condition
//! - INSERT INTO table (cols) VALUES (vals)
//! - DELETE FROM table WHERE condition
//!
//! Supported WHERE operators: =, <, >, <=, >=, !=

use scharnhorst_core::RowId;
use serde_json::Value;

use crate::diff::Diff;
use crate::error::{JournalError, JournalResult};

/// Represents a parsed SQL statement.
#[derive(Debug, Clone, PartialEq)]
pub enum SqlStatement {
    Update {
        table: String,
        column: String,
        value: SqlValue,
        condition: WhereCondition,
    },
    Insert {
        table: String,
        columns: Vec<String>,
        values: Vec<SqlValue>,
    },
    Delete {
        table: String,
        condition: WhereCondition,
    },
}

/// Represents a value in SQL (parsed from the statement).
#[derive(Debug, Clone, PartialEq)]
pub enum SqlValue {
    Integer(i64),
    Float(f64),
    String(String),
    Boolean(bool),
    Null,
}

impl SqlValue {
    /// Convert the SQL value to a JSON value for Diff storage.
    pub fn to_json(&self) -> Value {
        match self {
            SqlValue::Integer(i) => Value::Number((*i).into()),
            SqlValue::Float(f) => serde_json::Number::from_f64(*f)
                .map(Value::Number)
                .unwrap_or(Value::Null),
            SqlValue::String(s) => Value::String(s.clone()),
            SqlValue::Boolean(b) => Value::Bool(*b),
            SqlValue::Null => Value::Null,
        }
    }
}

/// Represents a WHERE condition.
#[derive(Debug, Clone, PartialEq)]
pub struct WhereCondition {
    pub column: String,
    pub operator: ComparisonOp,
    pub value: SqlValue,
}

/// Comparison operators supported in WHERE clauses.
#[derive(Debug, Clone, PartialEq)]
pub enum ComparisonOp {
    Eq, // =
    Lt, // <
    Gt, // >
    Le, // <=
    Ge, // >=
    Ne, // != or <>
}

/// SQL parser for debug write operations.
pub struct SqlParser;

impl SqlParser {
    /// Parse a SQL string into a SqlStatement.
    pub fn parse(sql: &str) -> JournalResult<SqlStatement> {
        let trimmed = sql.trim();
        if trimmed.is_empty() {
            return Err(JournalError::SqlParse("empty SQL statement".to_owned()));
        }

        // Determine statement type by looking at the first keyword
        let first_word = trimmed
            .split_whitespace()
            .next()
            .map(|s| s.to_ascii_uppercase())
            .unwrap_or_default();

        match first_word.as_str() {
            "UPDATE" => Self::parse_update(trimmed),
            "INSERT" => Self::parse_insert(trimmed),
            "DELETE" => Self::parse_delete(trimmed),
            _ => Err(JournalError::SqlUnsupported(format!(
                "unsupported SQL statement type: {}",
                first_word
            ))),
        }
    }

    /// Convert a SqlStatement into a Diff.
    ///
    /// For UPDATE and DELETE, the WHERE condition must identify a single row
    /// by primary key (e.g., `WHERE actor_id = 1`).
    pub fn statement_to_diff(stmt: SqlStatement) -> JournalResult<Diff> {
        match stmt {
            SqlStatement::Update {
                table,
                column,
                value,
                condition,
            } => {
                let row_id = Self::extract_row_id_from_condition(&condition)?;
                Ok(Diff::Update {
                    table,
                    row: row_id,
                    column,
                    value: value.to_json(),
                })
            }
            SqlStatement::Insert {
                table,
                columns,
                values,
            } => {
                if columns.len() != values.len() {
                    return Err(JournalError::SqlParse(
                        "column count does not match value count".to_owned(),
                    ));
                }

                // Find the row_id from the columns if present
                let mut row_id = None;
                let mut value_map = serde_json::Map::new();

                for (col, val) in columns.iter().zip(values.iter()) {
                    if col.eq_ignore_ascii_case("row_id")
                        || col.eq_ignore_ascii_case("id")
                        || col.eq_ignore_ascii_case("actor_id")
                    {
                        row_id = Some(Self::sql_value_to_row_id(val)?);
                    }
                    value_map.insert(col.clone(), val.to_json());
                }

                let row_id = row_id.ok_or_else(|| {
                    JournalError::SqlParse("INSERT must specify a row identifier column".to_owned())
                })?;

                Ok(Diff::Insert {
                    table,
                    row: row_id,
                    values: value_map,
                })
            }
            SqlStatement::Delete { table, condition } => {
                let row_id = Self::extract_row_id_from_condition(&condition)?;
                Ok(Diff::Delete { table, row: row_id })
            }
        }
    }

    /// Parse an UPDATE statement.
    ///
    /// Syntax: UPDATE table SET col = val WHERE condition
    fn parse_update(sql: &str) -> JournalResult<SqlStatement> {
        // Remove the UPDATE keyword
        let after_update = sql
            .trim()
            .strip_prefix("UPDATE")
            .or_else(|| sql.trim().strip_prefix("update"))
            .ok_or_else(|| JournalError::SqlParse("expected UPDATE".to_owned()))?;

        let mut tokens = Tokenizer::new(after_update);

        // Parse table name
        let table = tokens
            .next_identifier()
            .ok_or_else(|| JournalError::SqlParse("expected table name after UPDATE".to_owned()))?;

        // Expect SET
        if !tokens.consume_keyword("SET") {
            return Err(JournalError::SqlParse(
                "expected SET after table name".to_owned(),
            ));
        }

        // Parse column = value
        let column = tokens
            .next_identifier()
            .ok_or_else(|| JournalError::SqlParse("expected column name after SET".to_owned()))?;

        if !tokens.consume_operator("=") {
            return Err(JournalError::SqlParse(
                "expected = after column name".to_owned(),
            ));
        }

        let value = tokens
            .next_value()
            .ok_or_else(|| JournalError::SqlParse("expected value after =".to_owned()))?;

        // Expect WHERE
        if !tokens.consume_keyword("WHERE") {
            return Err(JournalError::SqlParse("expected WHERE clause".to_owned()));
        }

        let condition = Self::parse_where_clause(&mut tokens)?;

        Ok(SqlStatement::Update {
            table,
            column,
            value,
            condition,
        })
    }

    /// Parse an INSERT statement.
    ///
    /// Syntax: INSERT INTO table (col1, col2) VALUES (val1, val2)
    fn parse_insert(sql: &str) -> JournalResult<SqlStatement> {
        let after_insert = sql
            .trim()
            .strip_prefix("INSERT")
            .or_else(|| sql.trim().strip_prefix("insert"))
            .ok_or_else(|| JournalError::SqlParse("expected INSERT".to_owned()))?;

        let mut tokens = Tokenizer::new(after_insert);

        // Expect INTO
        if !tokens.consume_keyword("INTO") {
            return Err(JournalError::SqlParse(
                "expected INTO after INSERT".to_owned(),
            ));
        }

        // Parse table name
        let table = tokens
            .next_identifier()
            .ok_or_else(|| JournalError::SqlParse("expected table name after INTO".to_owned()))?;

        // Parse column list (col1, col2,...)
        let columns = tokens.next_paren_list().ok_or_else(|| {
            JournalError::SqlParse("expected column list (col1, col2, ...)".to_owned())
        })?;

        // Expect VALUES
        if !tokens.consume_keyword("VALUES") {
            return Err(JournalError::SqlParse(
                "expected VALUES after column list".to_owned(),
            ));
        }

        // Parse value list (val1, val2,...)
        let values = tokens.next_paren_values().ok_or_else(|| {
            JournalError::SqlParse("expected value list (val1, val2, ...)".to_owned())
        })?;

        if columns.len() != values.len() {
            return Err(JournalError::SqlParse(format!(
                "column count ({}) does not match value count ({})",
                columns.len(),
                values.len()
            )));
        }

        Ok(SqlStatement::Insert {
            table,
            columns,
            values,
        })
    }

    /// Parse a DELETE statement.
    ///
    /// Syntax: DELETE FROM table WHERE condition
    fn parse_delete(sql: &str) -> JournalResult<SqlStatement> {
        let after_delete = sql
            .trim()
            .strip_prefix("DELETE")
            .or_else(|| sql.trim().strip_prefix("delete"))
            .ok_or_else(|| JournalError::SqlParse("expected DELETE".to_owned()))?;

        let mut tokens = Tokenizer::new(after_delete);

        // Expect FROM
        if !tokens.consume_keyword("FROM") {
            return Err(JournalError::SqlParse(
                "expected FROM after DELETE".to_owned(),
            ));
        }

        // Parse table name
        let table = tokens
            .next_identifier()
            .ok_or_else(|| JournalError::SqlParse("expected table name after FROM".to_owned()))?;

        // Expect WHERE
        if !tokens.consume_keyword("WHERE") {
            return Err(JournalError::SqlParse("expected WHERE clause".to_owned()));
        }

        let condition = Self::parse_where_clause(&mut tokens)?;

        Ok(SqlStatement::Delete { table, condition })
    }

    /// Parse a WHERE clause: column op value
    fn parse_where_clause(tokens: &mut Tokenizer) -> JournalResult<WhereCondition> {
        let column = tokens
            .next_identifier()
            .ok_or_else(|| JournalError::SqlParse("expected column name in WHERE".to_owned()))?;

        let operator = tokens.next_comparison_op().ok_or_else(|| {
            JournalError::SqlParse("expected comparison operator in WHERE".to_owned())
        })?;

        let value = tokens
            .next_value()
            .ok_or_else(|| JournalError::SqlParse("expected value in WHERE".to_owned()))?;

        Ok(WhereCondition {
            column,
            operator,
            value,
        })
    }

    /// Extract a RowId from a WHERE condition that identifies a single row.
    ///
    /// Currently only supports simple equality conditions on id-like columns.
    fn extract_row_id_from_condition(condition: &WhereCondition) -> JournalResult<RowId> {
        // Only support equality operator for row identification
        if condition.operator != ComparisonOp::Eq {
            return Err(JournalError::SqlUnsupported(
                "WHERE clause must use = operator for row identification".to_owned(),
            ));
        }

        // Check if the column is an id-like column
        let col_lower = condition.column.to_ascii_lowercase();
        if !col_lower.ends_with("_id") && col_lower != "id" && col_lower != "row_id" {
            return Err(JournalError::SqlUnsupported(format!(
                "WHERE clause must use an id column for row identification, got: {}",
                condition.column
            )));
        }

        Self::sql_value_to_row_id(&condition.value)
    }

    /// Convert a SQL value to a RowId.
    fn sql_value_to_row_id(value: &SqlValue) -> JournalResult<RowId> {
        match value {
            SqlValue::Integer(i) if *i >= 0 => Ok(RowId::new(*i as u64)),
            _ => Err(JournalError::SqlTypeError {
                expected: "positive integer (row id)".to_owned(),
                actual: format!("{:?}", value),
            }),
        }
    }
}

/// Simple tokenizer for SQL parsing.
struct Tokenizer<'a> {
    input: &'a str,
    pos: usize,
}

impl<'a> Tokenizer<'a> {
    fn new(input: &'a str) -> Self {
        Self { input, pos: 0 }
    }

    /// Skip whitespace.
    fn skip_whitespace(&mut self) {
        while self.pos < self.input.len() {
            let c = self.input[self.pos..].chars().next().unwrap_or('\0');
            if c.is_whitespace() {
                self.pos += 1;
            } else {
                break;
            }
        }
    }

    /// Peek at the next non-whitespace character.
    fn peek_char(&mut self) -> Option<char> {
        self.skip_whitespace();
        self.input[self.pos..].chars().next()
    }

    /// Consume a keyword (case-insensitive).
    fn consume_keyword(&mut self, keyword: &str) -> bool {
        self.skip_whitespace();
        let remaining = &self.input[self.pos..];
        let keyword_len = keyword.len();

        if remaining.len() >= keyword_len {
            let prefix = &remaining[..keyword_len];
            if prefix.eq_ignore_ascii_case(keyword) {
                // Make sure it's a complete word (followed by whitespace or special char)
                if remaining.len() == keyword_len
                    || !remaining.as_bytes()[keyword_len].is_ascii_alphanumeric()
                        && remaining[keyword_len..].chars().next().unwrap_or('\0') != '_'
                {
                    self.pos += keyword_len;
                    return true;
                }
            }
        }
        false
    }

    /// Consume an operator.
    fn consume_operator(&mut self, op: &str) -> bool {
        self.skip_whitespace();
        let remaining = &self.input[self.pos..];
        if remaining.starts_with(op) {
            self.pos += op.len();
            return true;
        }
        false
    }

    /// Get the next identifier (table name, column name, etc.).
    fn next_identifier(&mut self) -> Option<String> {
        self.skip_whitespace();

        let remaining = &self.input[self.pos..];
        if remaining.is_empty() {
            return None;
        }

        let first_char = remaining.chars().next()?;
        if !first_char.is_ascii_alphabetic() && first_char != '_' {
            return None;
        }

        let end_pos = remaining
            .char_indices()
            .find(|(_, c)| !c.is_ascii_alphanumeric() && *c != '_')
            .map(|(i, _)| i)
            .unwrap_or(remaining.len());

        let ident = &remaining[..end_pos];
        self.pos += end_pos;
        Some(ident.to_owned())
    }

    /// Get the next comparison operator.
    fn next_comparison_op(&mut self) -> Option<ComparisonOp> {
        self.skip_whitespace();
        let remaining = &self.input[self.pos..];

        if remaining.starts_with("<=") {
            self.pos += 2;
            Some(ComparisonOp::Le)
        } else if remaining.starts_with(">=") {
            self.pos += 2;
            Some(ComparisonOp::Ge)
        } else if remaining.starts_with("!=") || remaining.starts_with("<>") {
            self.pos += 2;
            Some(ComparisonOp::Ne)
        } else if remaining.starts_with('<') {
            self.pos += 1;
            Some(ComparisonOp::Lt)
        } else if remaining.starts_with('>') {
            self.pos += 1;
            Some(ComparisonOp::Gt)
        } else if remaining.starts_with('=') {
            self.pos += 1;
            Some(ComparisonOp::Eq)
        } else {
            None
        }
    }

    /// Get the next value (integer, float, string, boolean, null).
    fn next_value(&mut self) -> Option<SqlValue> {
        self.skip_whitespace();
        let remaining = &self.input[self.pos..];

        if remaining.is_empty() {
            return None;
        }

        let first_char = remaining.chars().next()?;

        // String literal (single or double quotes)
        if first_char == '\'' || first_char == '"' {
            return self.parse_string_literal(first_char);
        }

        // Boolean or NULL
        if first_char.is_ascii_alphabetic() {
            return self.parse_keyword_value();
        }

        // Number (integer or float)
        if first_char.is_ascii_digit() || first_char == '-' || first_char == '+' {
            return self.parse_number();
        }

        None
    }

    /// Parse a string literal.
    fn parse_string_literal(&mut self, quote_char: char) -> Option<SqlValue> {
        self.skip_whitespace();
        let remaining = &self.input[self.pos..];

        if !remaining.starts_with(quote_char) {
            return None;
        }

        self.pos += 1; // Skip opening quote
        let start = self.pos;

        while self.pos < self.input.len() {
            let c = self.input[self.pos..].chars().next().unwrap_or('\0');
            if c == quote_char {
                // Check for escaped quote (double quote)
                if self.pos + 1 < self.input.len()
                    && self.input[self.pos + 1..].chars().next().unwrap_or('\0') == quote_char
                {
                    self.pos += 2;
                } else {
                    let value = &self.input[start..self.pos];
                    self.pos += 1; // Skip closing quote
                                   // Handle escaped quotes by replacing doubled quotes with single
                    let unescaped = value.replace(
                        &format!("{}{}", quote_char, quote_char),
                        &quote_char.to_string(),
                    );
                    return Some(SqlValue::String(unescaped));
                }
            } else {
                self.pos += 1;
            }
        }

        None // Unterminated string
    }

    /// Parse a keyword value (TRUE, FALSE, NULL).
    fn parse_keyword_value(&mut self) -> Option<SqlValue> {
        self.skip_whitespace();
        let remaining = &self.input[self.pos..];

        let word: String = remaining
            .chars()
            .take_while(|c| c.is_ascii_alphabetic())
            .collect();

        let word_upper = word.to_ascii_uppercase();
        let len = word.len();

        match word_upper.as_str() {
            "TRUE" => {
                self.pos += len;
                Some(SqlValue::Boolean(true))
            }
            "FALSE" => {
                self.pos += len;
                Some(SqlValue::Boolean(false))
            }
            "NULL" => {
                self.pos += len;
                Some(SqlValue::Null)
            }
            _ => None,
        }
    }

    /// Parse a number (integer or float).
    fn parse_number(&mut self) -> Option<SqlValue> {
        self.skip_whitespace();
        let remaining = &self.input[self.pos..];

        let mut end_pos = 0;
        let mut has_dot = false;

        for (i, c) in remaining.char_indices() {
            if c.is_ascii_digit() {
                end_pos = i + 1;
            } else if c == '.' && !has_dot && i > 0 {
                has_dot = true;
                end_pos = i + 1;
            } else if (c == '-' || c == '+') && i == 0 {
                end_pos = i + 1;
            } else {
                break;
            }
        }

        if end_pos == 0 || (end_pos == 1 && !remaining.chars().next()?.is_ascii_digit()) {
            return None;
        }

        let num_str = &remaining[..end_pos];
        self.pos += end_pos;

        if has_dot {
            num_str.parse::<f64>().ok().map(SqlValue::Float)
        } else {
            num_str.parse::<i64>().ok().map(SqlValue::Integer)
        }
    }

    /// Parse a parenthesized list of identifiers: (col1, col2, col3)
    fn next_paren_list(&mut self) -> Option<Vec<String>> {
        self.skip_whitespace();
        let remaining = &self.input[self.pos..];

        if !remaining.starts_with('(') {
            return None;
        }

        self.pos += 1; // Skip '('

        let mut items = Vec::new();
        loop {
            self.skip_whitespace();

            if self.peek_char() == Some(')') {
                self.pos += 1;
                break;
            }

            {
                let ident = self.next_identifier()?;
                items.push(ident);
            }

            self.skip_whitespace();

            // Check for comma or closing paren
            if self.peek_char() == Some(',') {
                self.pos += 1;
            } else if self.peek_char() == Some(')') {
                self.pos += 1;
                break;
            } else {
                return None;
            }
        }

        Some(items)
    }

    /// Parse a parenthesized list of values: (val1, val2, val3)
    fn next_paren_values(&mut self) -> Option<Vec<SqlValue>> {
        self.skip_whitespace();
        let remaining = &self.input[self.pos..];

        if !remaining.starts_with('(') {
            return None;
        }

        self.pos += 1; // Skip '('

        let mut values = Vec::new();
        loop {
            self.skip_whitespace();

            if self.peek_char() == Some(')') {
                self.pos += 1;
                break;
            }

            {
                let val = self.next_value()?;
                values.push(val);
            }

            self.skip_whitespace();

            // Check for comma or closing paren
            if self.peek_char() == Some(',') {
                self.pos += 1;
            } else if self.peek_char() == Some(')') {
                self.pos += 1;
                break;
            } else {
                return None;
            }
        }

        Some(values)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_simple_update() {
        let sql = "UPDATE actor_state SET treasury = 1000 WHERE actor_id = 1";
        let stmt = SqlParser::parse(sql).unwrap();

        match stmt {
            SqlStatement::Update {
                table,
                column,
                value,
                condition,
            } => {
                assert_eq!(table, "actor_state");
                assert_eq!(column, "treasury");
                assert_eq!(value, SqlValue::Integer(1000));
                assert_eq!(condition.column, "actor_id");
                assert_eq!(condition.operator, ComparisonOp::Eq);
                assert_eq!(condition.value, SqlValue::Integer(1));
            }
            _ => panic!("expected UPDATE statement"),
        }
    }

    #[test]
    fn parse_insert() {
        let sql = "INSERT INTO actor_state (actor_id, treasury, name) VALUES (1, 5000, 'France')";
        let stmt = SqlParser::parse(sql).unwrap();

        match stmt {
            SqlStatement::Insert {
                table,
                columns,
                values,
            } => {
                assert_eq!(table, "actor_state");
                assert_eq!(columns, vec!["actor_id", "treasury", "name"]);
                assert_eq!(values.len(), 3);
                assert_eq!(values[0], SqlValue::Integer(1));
                assert_eq!(values[1], SqlValue::Integer(5000));
                assert_eq!(values[2], SqlValue::String("France".to_owned()));
            }
            _ => panic!("expected INSERT statement"),
        }
    }

    #[test]
    fn parse_delete() {
        let sql = "DELETE FROM actor_state WHERE actor_id = 5";
        let stmt = SqlParser::parse(sql).unwrap();

        match stmt {
            SqlStatement::Delete { table, condition } => {
                assert_eq!(table, "actor_state");
                assert_eq!(condition.column, "actor_id");
                assert_eq!(condition.operator, ComparisonOp::Eq);
                assert_eq!(condition.value, SqlValue::Integer(5));
            }
            _ => panic!("expected DELETE statement"),
        }
    }

    #[test]
    fn parse_string_values() {
        let sql = "UPDATE provinces SET name = 'Paris' WHERE province_id = 10";
        let stmt = SqlParser::parse(sql).unwrap();

        match stmt {
            SqlStatement::Update { value, .. } => {
                assert_eq!(value, SqlValue::String("Paris".to_owned()));
            }
            _ => panic!("expected UPDATE statement"),
        }
    }

    #[test]
    fn parse_float_values() {
        let sql = "UPDATE provinces SET tax_rate = 0.15 WHERE province_id = 1";
        let stmt = SqlParser::parse(sql).unwrap();

        match stmt {
            SqlStatement::Update { value, .. } => {
                assert_eq!(value, SqlValue::Float(0.15));
            }
            _ => panic!("expected UPDATE statement"),
        }
    }

    #[test]
    fn parse_boolean_values() {
        let sql = "UPDATE actor_state SET is_at_war = TRUE WHERE actor_id = 1";
        let stmt = SqlParser::parse(sql).unwrap();

        match stmt {
            SqlStatement::Update { value, .. } => {
                assert_eq!(value, SqlValue::Boolean(true));
            }
            _ => panic!("expected UPDATE statement"),
        }
    }

    #[test]
    fn parse_null_value() {
        let sql = "UPDATE actor_state SET alliance_id = NULL WHERE actor_id = 1";
        let stmt = SqlParser::parse(sql).unwrap();

        match stmt {
            SqlStatement::Update { value, .. } => {
                assert_eq!(value, SqlValue::Null);
            }
            _ => panic!("expected UPDATE statement"),
        }
    }

    #[test]
    fn parse_case_insensitive() {
        let sql = "update actor_state set treasury = 100 where actor_id = 1";
        let stmt = SqlParser::parse(sql).unwrap();

        match stmt {
            SqlStatement::Update { table, column, .. } => {
                assert_eq!(table, "actor_state");
                assert_eq!(column, "treasury");
            }
            _ => panic!("expected UPDATE statement"),
        }
    }

    #[test]
    fn statement_to_diff_update() {
        let sql = "UPDATE actor_state SET treasury = 1000 WHERE actor_id = 1";
        let stmt = SqlParser::parse(sql).unwrap();
        let diff = SqlParser::statement_to_diff(stmt).unwrap();

        match diff {
            Diff::Update {
                table,
                row,
                column,
                value,
            } => {
                assert_eq!(table, "actor_state");
                assert_eq!(row, RowId::new(1));
                assert_eq!(column, "treasury");
                assert_eq!(value, serde_json::json!(1000));
            }
            _ => panic!("expected Update diff"),
        }
    }

    #[test]
    fn statement_to_diff_insert() {
        let sql = "INSERT INTO actor_state (actor_id, treasury) VALUES (5, 10000)";
        let stmt = SqlParser::parse(sql).unwrap();
        let diff = SqlParser::statement_to_diff(stmt).unwrap();

        match diff {
            Diff::Insert { table, row, values } => {
                assert_eq!(table, "actor_state");
                assert_eq!(row, RowId::new(5));
                assert_eq!(values.get("treasury"), Some(&serde_json::json!(10000)));
            }
            _ => panic!("expected Insert diff"),
        }
    }

    #[test]
    fn statement_to_diff_delete() {
        let sql = "DELETE FROM actor_state WHERE actor_id = 3";
        let stmt = SqlParser::parse(sql).unwrap();
        let diff = SqlParser::statement_to_diff(stmt).unwrap();

        match diff {
            Diff::Delete { table, row } => {
                assert_eq!(table, "actor_state");
                assert_eq!(row, RowId::new(3));
            }
            _ => panic!("expected Delete diff"),
        }
    }

    #[test]
    fn parse_unsupported_statement() {
        let sql = "SELECT * FROM actor_state";
        let result = SqlParser::parse(sql);
        assert!(result.is_err());
    }

    #[test]
    fn parse_empty_sql() {
        let result = SqlParser::parse("");
        assert!(result.is_err());
    }

    #[test]
    fn parse_where_with_comparison_operators() {
        // Test various comparison operators - note: only = is supported for row identification
        let sql = "UPDATE actor_state SET treasury = 100 WHERE actor_id = 1";
        let stmt = SqlParser::parse(sql).unwrap();

        match stmt {
            SqlStatement::Update { condition, .. } => {
                assert_eq!(condition.operator, ComparisonOp::Eq);
            }
            _ => panic!("expected UPDATE statement"),
        }
    }
}
