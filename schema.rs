/*
 * schema.rs
 *
 * Implements reading EasyDB schema from a file
 *
 * University of Toronto
 * 2019
 */

use std::fs;
use std::io;
use std::fmt;
use crate::packet::Value;

pub struct ColumnMetadata {
    pub c_name: String, /* column name */
    pub c_id: i32,      /* column id */
    pub c_type: i32,    /* one of 4 native types */
    pub c_ref: i32,     /* referenced table id (only applicable to Foreign types) */
}

pub struct TableMetadata {
    pub t_name: String,
    pub t_id: i32,
    pub t_cols: Vec<ColumnMetadata>,
}

impl ColumnMetadata {
    fn new(cname: String, cid: i32, ctype: i32, cref: i32) -> ColumnMetadata {
        ColumnMetadata {
            c_name: cname,
            c_id:   cid,
            c_type: ctype,
            c_ref:  cref,
        }
    }

    fn type_as_str(& self) -> String {
        match self.c_type {
            Value::INTEGER => String::from("integer"),
            Value::FLOAT => String::from("float"),
            Value::STRING => String::from("string"),
            Value::FOREIGN => format!("foreign({})", self.c_ref),
            _ => String::from("unknown"),
        }
    }
}

impl TableMetadata {
    fn new(name: String, tid: i32, tcols: Vec<ColumnMetadata>) -> TableMetadata {
        TableMetadata {
            t_name: name,
            t_id: tid,
            t_cols: tcols,
        }
    }
}

/* Returns each string in a vector from the schema file */
pub fn tokenize(filename: &String) -> io::Result<Vec<String>> {
    let contents = fs::read_to_string(filename)?;
    let mut tokens = vec![];
    let mut token = String::new();

    for ch in contents.chars() {
        if ch.is_alphabetic() {
            token.push(ch);
        }
        else if ch.is_numeric() || ch == '_' {
            if token.len() == 0 {
                return Err(io::Error::new(io::ErrorKind::Other,
                    "invalid identifier, cannot start with a number or underscore"));
            }
            token.push(ch);
        }
        else {
            if token.len() > 0 {
                tokens.push(token);
                token = String::new();
            }

            if !ch.is_whitespace() {
                tokens.push(ch.to_string());
            }
        }
    }

    Ok(tokens)
}

fn validate_name(name: & String) -> Result<& String, &'static str>
{
    for ch in name.chars() {
        if !ch.is_alphanumeric() && ch != '_' {
            return Err("invalid name identifier");
        }
    }
    Ok(name)
}

/* 
 * Parses one column in the schema, and returns the initialized column 
 * if well formed, else return an error 
 */
fn parse_column<'a, I>(it: &mut I, column_id: i32, tables: & Vec<TableMetadata>)
    -> Result<Option<ColumnMetadata>, &'static str>
    where I: Iterator<Item = &'a String>,
{
    /* Check for ending curly bracket */
    let column_name = match it.next() {
        Some(column_name) => match column_name.as_str() {
            "}" => return Ok(None),
            _ => validate_name(column_name)?,
        },
        None => return Err("unexpected end of file"),
    };

    /* Check for colon after the column name */
    match it.next() {
        Some(tok) => match tok.as_str() {
            ":" => (),
            _ => return Err("expecting ':' after column name"),
        }
        None => return Err("unexpected end of file"),
    };

    let column_type = match it.next() {
        Some(column_type) => column_type,
        None => return Err("unexpected end of file"),
    };

    /* Check for semi colon after each column */
    match it.next() {
        Some(tok) => match tok.as_str() {
            ";" => (),
            _ => return Err("expecting ';' after column type"),
        }
        None => return Err("unexpected end of file"),
    };

    /* Parse one column and return */
    let column = match column_type.as_str() {
        "integer" =>
            ColumnMetadata::new(column_name.to_string(), column_id, Value::INTEGER, 0),
        "float" =>
            ColumnMetadata::new(column_name.to_string(), column_id, Value::FLOAT, 0),
        "string" =>
            ColumnMetadata::new(column_name.to_string(), column_id, Value::STRING, 0),
        _ => {
            let index = match tables.iter().position(|t|
                column_type.as_str() == t.t_name) {
                Some(index) => index,
                None => return Err("cannot find reference table"),
            };
            ColumnMetadata::new(column_name.to_string(), column_id, Value::FOREIGN,
                index as i32 + 1)
        }
    };

    Ok(Some(column))
}

/* Parses a single table with columns, and returns the initialized table */
fn parse_table<'a, I>(it: &mut I, tables: & Vec<TableMetadata>)
    -> Result<Option<TableMetadata>, &'static str>
    where I: Iterator<Item = &'a String>,
{
    let mut columns: Vec<ColumnMetadata> = vec![];

    /* Check for a valid table name */
    let table_name = match it.next() {
        Some(table_name) => validate_name(table_name)?,
        None => return Ok(None),
    };

    /* Check for curly brackets and columns inside table */
    match it.next() {
        Some(tok) => match tok.as_str() {
            "{" => (),
            _ => return Err("expecting '{' after table name"),
        },
        None => return Err("unexpected end of file"),
    };

    loop {
        let column_id = columns.len() as i32 + 1;
        let column = match parse_column(it, column_id, tables)? {
            Some(column) => column,
            None => break,
        };
        columns.push(column);
    }

    if columns.len() == 0 {
        Err("table has no column")
    }
    else {
        let table_id = tables.len() as i32 + 1;
        Ok(Some(TableMetadata::new(table_name.to_string(), table_id, columns)))
    }
}

/* 
 * Iteratively parses each table from the vector of tokens and returns 
 * the vector of tables 
 */
pub fn parse(tokens: Vec<String>) -> Result<Vec<TableMetadata>, &'static str> {
    let mut tables: Vec<TableMetadata> = vec![];

    let mut it = tokens.iter();
    loop {
        let table = match parse_table(&mut it, &tables)? {
            Some(table) => table,
            None => break,
        };
        tables.push(table);
    }

    if tables.len() == 0 {
        Err("schema file is empty")
    }
    else {
        Ok(tables)
    }
}

/* For debugging */
impl fmt::Display for ColumnMetadata {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}. {} : {}", self.c_id, self.c_name, self.type_as_str())
    }
}

impl fmt::Debug for TableMetadata {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "\n{}. {}:\n", self.t_id, self.t_name)?;
        for column in &self.t_cols {
            write!(f, "    {}\n", column)?;
        }
        Ok(())
    }
}


