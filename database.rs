/*
 * database.rs
 *
 * Implementation of EasyDB database internals
 *
 * University of Toronto
 * 2019
 */

use crate::packet::{Command, Request, Response, Value};
use crate::schema::TableMetadata;
use crate::schema::ColumnMetadata;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::{Mutex, MutexGuard};

/* OP codes for the query command */
pub const OP_AL: i32 = 1;
pub const OP_EQ: i32 = 2;
pub const OP_NE: i32 = 3;
pub const OP_LT: i32 = 4;
pub const OP_GT: i32 = 5;
pub const OP_LE: i32 = 6;
pub const OP_GE: i32 = 7;

struct DatabaseTableRow {
    version: i64,
    data: Vec<Value>,
}

pub struct DatabaseTable {
    table_schema: TableMetadata,
    rows: Mutex<HashMap<i64, DatabaseTableRow>>,
    /* Each row ID is mapped to a structure containing the row's data */
}

/* You can implement your Database structure here
 * Q: How you will store your tables into the database? */
pub struct DatabaseData {
    tables: Vec<DatabaseTable>,
    next_row_key: Mutex<i64>,

    // The drop operation requires that any rows that make a foreign reference to another row in the table be deleted
    // when the referenced row is deleted.
    //
    // In order to implement this functionality efficiently, the following map is used to keep track of all rows that
    // make a foreign reference to some specific row
    // This mapping takes a row ID as the key, and returns a set of all the rows (as a (table_id, row_id) pair) that
    // reference the row specified in the key
    foreign_references_map: Mutex<HashMap<i64, HashSet<(i32, i64)>>>,
}

/*pub struct DatabaseMutexes {
    mutex_tables: Vec<Mutex<bool>>,
    mutex_row_key: Mutex<bool>,
    mutex_foreign_ref_map: Mutex<bool>,
}*/

pub struct Database {
    //mt: DatabaseMutexes,
    db: DatabaseData,
}

impl Database {
    pub fn new(tables_schema: Vec<TableMetadata>) -> Database {
        let mut db = DatabaseData {tables: vec![], next_row_key: Mutex::new(1), foreign_references_map: Mutex::new(HashMap::new()) };
        // let mut mt = DatabaseMutexes {mutex_tables: vec![], mutex_row_key: Mutex::new(false), mutex_foreign_ref_map: Mutex::new(false)};

        for table_schema in tables_schema {
            db.tables.push(DatabaseTable { table_schema, rows: Mutex::new(HashMap::new()) });
            // mt.mutex_tables.push(Mutex::new(false));
        }

        let db = Database { /*mt,*/ db};
        db
    }
}

/* Receive the request packet from client and send a response back */
pub fn handle_request(request: Request, db: &Database)
                      -> Response {
    /* Handle a valid request */
    let result = match request.command {
        Command::Insert(values) => handle_insert(db, request.table_id, values),
        Command::Update(id, version, values) => handle_update(db, request.table_id, id, version, values),
        Command::Drop(id) => handle_drop(db, request.table_id, id),
        Command::Get(id) => handle_get(db, request.table_id, id),
        Command::Query(column_id, operator, value) => handle_query(db, request.table_id, column_id, operator, value),
        /* should never get here */
        Command::Exit => Err(Response::UNIMPLEMENTED),
    };

    /* Send back a response */
    match result {
        Ok(response) => response,
        Err(code) => Response::Error(code),
    }
}

fn handle_insert(db: &Database, table_id: i32, values: Vec<Value>)
                 -> Result<Response, i32> {
    // Ensure that the specified table ID actually references a valid table
    if !validate_table_id(&db.db.tables, table_id) {
        return Err(Response::BAD_TABLE);
    }

    // Table IDs start at 1, but we want to use this ID to index into the database's tables vector
    // Hence, we convert it to a 0-based index and then fetch the corresponding table
    let table_id_0_based = (table_id - 1) as usize;
    let target_table = db.db.tables.get(table_id_0_based).unwrap();

    let target_table_cols = &target_table.table_schema.t_cols;

    // Validate the values according to the table schema of the target table
    if let Err(error_code) = validate_values_against_schema(&values, target_table_cols, &db.db) {
        return Err(error_code);
    }

    // Validation complete - now we will insert the new row into the database using the next available row key

    // First determine which key to use
    let mut row_key_mutex = db.db.next_row_key.lock().unwrap();
    let inserted_row_key = *row_key_mutex;

    // Update the row key for subsequent insertions
    *row_key_mutex += 1;
    drop(row_key_mutex);

    // Now update the database's foreign reference map by adding any foreign references made by this new row to the map
    let foreign_referenced_rows = get_all_referenced_rows(&values);
    let mut foreign_ref_map = db.db.foreign_references_map.lock().unwrap();

    add_to_foreign_reference_map(&mut *foreign_ref_map, &foreign_referenced_rows, inserted_row_key, table_id);

    drop(foreign_ref_map);

    // Now insert the row into the table
    let mut target_table_rows = target_table.rows.lock().unwrap();
    target_table_rows.insert(inserted_row_key, DatabaseTableRow { version: 1, data: values });

    Ok(Response::Insert(inserted_row_key, 1))
}

fn handle_update(db: &Database, table_id: i32, object_id: i64, version: i64, new_values: Vec<Value>)
                 -> Result<Response, i32> {
    // Ensure that the specified table ID actually references a valid table
    if !validate_table_id(&db.db.tables, table_id) {
        return Err(Response::BAD_TABLE);
    }

    let table_id_0_based = (table_id - 1) as usize;

    // Table IDs start at 1, but we want to use this ID to index into the database's tables vector
    // Hence, we convert it to a 0-based index and then fetch the corresponding table
    let target_table = db.db.tables.get(table_id_0_based).unwrap();
    let target_table_cols = &target_table.table_schema.t_cols;

    // Validate the values according to the table schema of the target table
    if let Err(error_code) = validate_values_against_schema(&new_values, target_table_cols, &db.db) {
        return Err(error_code);
    }

    // Validation complete - now we will update the specified row using the new values

    // let foreign_ref_map_mutex = db.mt.mutex_foreign_ref_map.lock().unwrap();
    let mut foreign_ref_map = db.db.foreign_references_map.lock().unwrap();
    let mut target_table_rows = target_table.rows.lock().unwrap();

    // Check that the row specified by object_id actually exists in the table specified by table_id
    // Note that the row ids are zero-indexed in Rust. since we started indexed them from 1 from Database::new()
    if !target_table_rows.contains_key(&object_id) {
        return Err(Response::NOT_FOUND);
    }

    // Check that the input version number matches the version number of this row in the database
    // If version number is 0, do the update regardless
    let target_row = target_table_rows.get(&object_id).unwrap();
    if target_row.version != version && version != 0 {
        return Err(Response::TXN_ABORT)
    }

    let prev_foreign_referenced_rows = get_all_referenced_rows(&target_row.data);
    let new_foreign_referenced_rows = get_all_referenced_rows(&new_values);
    // Before updating the table, we update the database's foreign reference map. This involves two steps:
    //
    // 1) Remove any mappings in the database's foreign reference map that correspond to the
    // target row's old values (as they are invalid now)
    remove_from_foreign_reference_map(&mut *foreign_ref_map, &prev_foreign_referenced_rows, object_id, table_id);
    //
    // 2) Add any new foreign references made by the row to the database's foreign references map
    add_to_foreign_reference_map(&mut *foreign_ref_map, &new_foreign_referenced_rows, object_id, table_id);

    drop(foreign_ref_map);

    // Now we can update the row in the database
    let new_version_num = version + 1;

    // Update the row in the database
    if let Some(db_table_row) = target_table_rows.get_mut(&object_id) {
        db_table_row.data = new_values;
        db_table_row.version = new_version_num;
    }

    Ok(Response::Update(new_version_num))
}

fn handle_drop(db: &Database, table_id: i32, object_id: i64)
               -> Result<Response, i32> {

    // let foreign_ref_map_mutex = db.mt.mutex_foreign_ref_map.lock().unwrap();
    let mut foreign_ref_map = db.db.foreign_references_map.lock().unwrap();

    let ret = drop_helper(&db.db.tables, &mut foreign_ref_map, table_id, object_id);

    ret
}

fn drop_helper(db_tables: &Vec<DatabaseTable>, foreign_ref_map: &mut MutexGuard<HashMap<i64, HashSet<(i32, i64)>>>, table_id: i32, object_id: i64) -> Result<Response, i32> {
    // Ensure that the specified table ID actually references a valid table
    if !validate_table_id(db_tables, table_id) {
        return Err(Response::BAD_TABLE);
    }

    // Table IDs start at 1, but we want to use this ID to index into the database's tables vector
    // Hence, we convert it to a 0-based index
    let table_id_0_based = (table_id - 1) as usize;

    //let table_mutex = db.mt.mutex_tables.get_mut(table_id_0_based).unwrap().lock().unwrap();
    let target_table = db_tables.get(table_id_0_based).unwrap();
    let mut target_table_rows = target_table.rows.lock().unwrap();

    // Remove the specified row from the table
    let removed_row = target_table_rows.remove(&object_id);
    if let None = removed_row {
        return Err(Response::NOT_FOUND);
    }

    // Get a list of all rows that made a foreign reference to the row we just deleted
    let referencing_rows = foreign_ref_map.remove(&object_id);
    let referencing_rows = match referencing_rows {
        Some(row_ids) => row_ids,
        _ => return Ok(Response::Drop)
    };

    // Recursively delete these rows
    for (referencing_table_id, referencing_row_id) in referencing_rows {
        // Recursively drop the referenced row
        let _ = drop_helper(db_tables, foreign_ref_map, referencing_table_id, referencing_row_id);
    }

    //drop(table_mutex);

    Ok(Response::Drop)
}

fn handle_get(db: &Database, table_id: i32, object_id: i64)
              -> Result<Response, i32> {
    // Ensure that the specified table ID actually references a valid table
    if !validate_table_id(&db.db.tables, table_id) {
        return Err(Response::BAD_TABLE);
    }

    // Table IDs start at 1, but we want to use this ID to index into the database's tables vector
    // Hence, we convert it to a 0-based index
    let table_id_0_based = (table_id - 1) as usize;

    // let table_mutex = db.mt.mutex_tables.get(table_id_0_based).unwrap().lock().unwrap();

    let target_table = db.db.tables.get(table_id_0_based).unwrap();
    let target_table_rows = target_table.rows.lock().unwrap();
    let target_row = target_table_rows.get(&object_id);
    let target_row = match target_row {
        Some(row) => row,
        _ => return Err(Response::NOT_FOUND)
    };

    let mut res = vec![];

    for val in &target_row.data {
        res.push(val.clone());
    }

    Ok(Response::Get(target_row.version, res))
}

fn handle_query(db: &Database, table_id: i32, column_id: i32, operator: i32, other: Value)
                -> Result<Response, i32> {
    // Ensure that the specified table ID actually references a valid table
    if !validate_table_id(&db.db.tables, table_id) {
        return Err(Response::BAD_TABLE);
    }

    // Table IDs start at 1, but we want to use this ID to index into the database's tables vector
    // Hence, we convert it to a 0-based index
    let table_id_0_based = (table_id - 1) as usize;

    let target_table = db.db.tables.get(table_id_0_based).unwrap();
    let target_table_rows = target_table.rows.lock().unwrap();
    let target_table_cols = &target_table.table_schema.t_cols;

    if operator == OP_AL {
        // For the AL operator, we require column_id to be 0
        if column_id != 0 {
            return Err(Response::BAD_QUERY);
        }
        else {
            // this holds all valid ids for the query
            let mut res: Vec<i64> = Vec::new();
            for (id, _row) in target_table_rows.iter() {
                res.push(*id);
            }

            return Ok(Response::Query(res));
        }
    }
    // check if column_id is out-of-bound for the table specified by table_id
    else if !validate_column_id(&*target_table, column_id) {
        return Err(Response::BAD_QUERY);
    }

    // Convert the column_id to a 0-based index (it is currently a 1-based index) so we can index the table's schema's
    // column type vector
    let column_id = (column_id - 1) as usize;

    // recall that it is possible to scan a table for id of a row, using column_id 0
    
    // this holds all valid ids for the query
    let mut res: Vec<i64> = Vec::new(); 

    // get the column type from column_id (there could be a bug where column_id is 0)
    let target_col_type = target_table_cols[column_id as usize].c_type;
    
    // check if the type of other matches the column type
    match other {
        Value::Null => {
            if target_col_type != Value::NULL {
                return Err(Response::BAD_QUERY);
            }
        },
        Value::Integer(_) => {
            if target_col_type != Value::INTEGER {
                return Err(Response::BAD_QUERY);
            }
        },
        Value::Float(_) => {
            if target_col_type != Value::FLOAT {
                return Err(Response::BAD_QUERY);
            }
        },
        Value::Text(_) => {
            if target_col_type != Value::STRING {
                return Err(Response::BAD_QUERY);
            }
        },
        Value::Foreign(_) => {
            if target_col_type != Value::FOREIGN {
                return Err(Response::BAD_QUERY);
            }
        },
    };

    match operator {
        OP_EQ => 
                for (id, row) in target_table_rows.iter() {
                    if column_id == 0 {
                        // it is possible to scan a table for id of a row, using column_id 0
                        if let Value::Integer(value) = other{
                            if *id == value {
                                res.push(*id);
                            }
                        }
                    }
                    if row.data[column_id as usize] == other {
                        res.push(*id)
                    };                
                },
        OP_NE => 
                for (id, row) in target_table_rows.iter() {
                    if column_id == 0 {
                        // it is possible to scan a table for id of a row, using column_id 0
                        if let Value::Integer(value) = other{
                            if *id == value {
                                res.push(*id);
                            }
                        }
                    }
                    if row.data[column_id as usize] != other {
                        res.push(*id)
                    };                
                },
        OP_LT => // column id and foreign fields only supported EQ and NE operators.
                // return error for all other operator types
                if target_col_type == Value::FOREIGN || column_id == 0 {
                    return Err(Response::BAD_QUERY);
                }
                else {
                    for (id, row) in target_table_rows.iter() {
                        if row.data[column_id as usize] < other {
                            res.push(*id);
                        }
                    }
                },
        OP_GT => // column id and foreign fields only supported EQ and NE operators.
                // return error for all other operator types
                if target_col_type == Value::FOREIGN || column_id == 0 {
                    return Err(Response::BAD_QUERY);
                }
                else {
                    for (id, row) in target_table_rows.iter() {
                        if row.data[column_id as usize] > other {
                            res.push(*id);
                        }
                    }
                },
        OP_LE => // column id and foreign fields only supported EQ and NE operators.
                 // return error for all other operator types
                if target_col_type == Value::FOREIGN || column_id == 0 {
                    return Err(Response::BAD_QUERY);
                }
                else {
                    for (id, row) in target_table_rows.iter() {
                        if row.data[column_id as usize] <= other {
                            res.push(*id);
                        }
                    }
                },
        OP_GE => // column id and foreign fields only supported EQ and NE operators.
                // return error for all other operator types
                if target_col_type == Value::FOREIGN || column_id == 0 {
                    return Err(Response::BAD_QUERY);
                }
                else {
                    for (id, row) in target_table_rows.iter() {
                        if row.data[column_id as usize] >= other {
                            res.push(*id);
                        }
                    }
                },
        // checks if operator number is valid 
        _ => return Err(Response::BAD_QUERY)
    };

    // drop(table_mutex);

    Ok(Response::Query(res))
}

fn validate_table_id(db_tables: &Vec<DatabaseTable>, table_id: i32) -> bool {
    // Ensure that the specified table ID actually references a valid table
    return table_id >= 1 && table_id <= (db_tables.len() as i32);
}

fn validate_column_id(target_table: &DatabaseTable, col_id: i32) -> bool {
    // Ensure that the specified column ID actually references a valid column
    return col_id >= 1 && col_id <= (target_table.table_schema.t_cols.len() as i32);
}

fn validate_values_against_schema(values: &Vec<Value>, table_cols: &Vec<ColumnMetadata>, db: &DatabaseData) -> Result<bool, i32> {
    // The number of values specified must match how many columns there are in the table to store these values
    if values.len() != table_cols.len() {
        return Err(Response::BAD_ROW);
    }

    // Ensure that the type of each value matches what is required in the table schema
    for i in 0..values.len() {
        match values[i] {
            Value::Null => {
                if table_cols[i].c_type != Value::NULL {
                    return Err(Response::BAD_VALUE);
                }
            }
            Value::Integer(_) => {
                if table_cols[i].c_type != Value::INTEGER {
                    return Err(Response::BAD_VALUE);
                }
            }
            Value::Float(_) => {
                if table_cols[i].c_type != Value::FLOAT {
                    return Err(Response::BAD_VALUE);
                }
            }
            Value::Text(_) => {
                if table_cols[i].c_type != Value::STRING {
                    return Err(Response::BAD_VALUE);
                }
            }
            Value::Foreign(referenced_row_id) => {
                if table_cols[i].c_type != Value::FOREIGN {
                    return Err(Response::BAD_VALUE);
                } else if referenced_row_id != 0 {
                    // Fetch the referenced table ID and convert it to a 0-based index
                    let referenced_table_index = (table_cols[i].c_ref - 1) as usize;

                    // Check that the referenced row actually exists in the table that the column refers to
                    let referenced_table = db.tables.get(referenced_table_index).unwrap();
                    let referenced_table_rows = referenced_table.rows.lock().unwrap();
                    if !referenced_table_rows.contains_key(&referenced_row_id) {
                        return Err(Response::BAD_FOREIGN);
                    }
                }
            }
        };
    }

    Ok(true)
}


/// Generates and returns a set consisting of all values that are a valid foreign reference (i.e. not a blank foreign
/// reference with referenced row ID == 0) from the list of values contained in a row

fn get_all_referenced_rows(values: &Vec<Value>) -> HashSet<i64> {
    // List of all the row IDs that this row makes reference to
    let mut foreign_referenced_rows: HashSet<i64> = HashSet::new();

    // Iterate through every value in values, and add the values of type Foreign to the set above
    for i in 0..values.len() {
        if let Value::Foreign(referenced_row_id) = &values[i] {
            // A foreign reference ID of 0 means that the row is not referencing any other row in the database
            // Hence, we only add a reference to the set if the referenced row ID is non-zero
            if *referenced_row_id != 0 {
                foreign_referenced_rows.insert(*referenced_row_id);
            }
        }
    }

    foreign_referenced_rows
}


/// Updates the foreign references map of the passed-in database by adding mappings for all the references made by
/// the row with ID 'referencing_row_id' (note: these referenced rows are passed in the 'referenced_rows' set)

fn add_to_foreign_reference_map(foreign_ref_map: &mut HashMap<i64, HashSet<(i32, i64)>>, referenced_rows: &HashSet<i64>, referencing_row_id: i64, referencing_row_table_id: i32) {
    for foreign_reference_id in referenced_rows {
        if !foreign_ref_map.contains_key(&foreign_reference_id) {
            // Create a new list for the referenced row that consists of the newly added row if it doesn't already exist
            foreign_ref_map.insert(*foreign_reference_id, HashSet::new());
        }

        // Append the newly added row to the referenced row's existing list of references
        foreign_ref_map.get_mut(foreign_reference_id).unwrap().insert((referencing_row_table_id, referencing_row_id));
    }
}


/// Removes mappings in the foreign references map of the passed-in database for all the references made by
/// the row with ID 'referencing_row_id' (note: these referenced rows are passed in the 'referenced_rows' set)

fn remove_from_foreign_reference_map(foreign_ref_map: &mut HashMap<i64, HashSet<(i32, i64)>>, referenced_rows: &HashSet<i64>, referencing_row_id: i64, referencing_row_table_id: i32) {
    for foreign_reference_id in referenced_rows {
        foreign_ref_map.get_mut(foreign_reference_id).unwrap().remove(&(referencing_row_table_id, referencing_row_id));
    }
}