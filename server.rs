/*
 * server.rs
 *
 * Implementation of EasyDB database server
 *
 * University of Toronto
 * 2019
 */

use std::net::TcpListener;
use std::net::TcpStream;
use std::io::Write;
use std::{io, thread};
use crate::packet::Command;
use crate::packet::Response;
use crate::packet::Network;
use crate::schema::TableMetadata;
use crate::database;
use crate::database::Database;
use std::sync::{Arc, Mutex};
use std::borrow::BorrowMut;
use std::rc::Rc;

fn single_threaded(listener: TcpListener, table_schema: Vec<TableMetadata>, verbose: bool) {
    // Initialize the database object using the specified table schema
    let db = Arc::new(Database::new(table_schema));

    for stream in listener.incoming() {
        let stream = stream.unwrap();

        if verbose {
            println!("Connected to {}", stream.peer_addr().unwrap());
        }

        match handle_connection(stream, db.clone()) {
            Ok(()) => {
                if verbose {
                    println!("Disconnected.");
                }
            }
            Err(e) => eprintln!("Connection error: {:?}", e),
        };
    }
}

fn multi_threaded(listener: TcpListener, table_schema: Vec<TableMetadata>, verbose: bool) {
    // Initialize the database object using the specified table schema
    let db = Arc::new(Database::new(table_schema));

    for stream in listener.incoming() {
        let thread_db_ref = db.clone();
        thread::spawn(move || {
            let stream = stream.unwrap();

            if verbose {
                println!("Connected to {}", stream.peer_addr().unwrap());
            }

            match handle_connection(stream, thread_db_ref) {
                Ok(()) => {
                    if verbose {
                        println!("Disconnected.");
                    }
                }
                Err(e) => eprintln!("Connection error: {:?}", e),
            };
        });
    }
}

/* Sets up the TCP connection between the database client and server */
pub fn run_server(table_schema: Vec<TableMetadata>, ip_address: String, verbose: bool) {
    let listener = match TcpListener::bind(ip_address) {
        Ok(listener) => listener,
        Err(e) => {
            eprintln!("Could not start server: {}", e);
            return;
        }
    };

    if verbose {
        println!("Listening: {:?}", listener);
    }

    //single_threaded(listener, table_schema, verbose);
    multi_threaded(listener, table_schema, verbose);
}

impl Network for TcpStream {}

/* Receive the request packet from ORM and send a response back */
fn handle_connection(mut stream: TcpStream, db: Arc<Database>)
                     -> io::Result<()> {
    // If more than 4 worker threads have been spawned, return SERVER_BUSY
    // Note: 5 is used in the condition because one reference to the database is reserved by the main thread
    // for the purpose of spawning other worker threads
    if Arc::strong_count(&db) > 5 {
        stream.respond(&Response::Error(Response::SERVER_BUSY))?;
        return Ok(());
    }

    // Tells the client that the connection to server is successful.
    stream.respond(&Response::Connected)?;

    loop {
        let request = match stream.receive() {
            Ok(request) => request,
            Err(e) => {
                /* respond error */
                stream.respond(&Response::Error(Response::BAD_REQUEST))?;
                return Err(e);
            }
        };

        /* we disconnect with client upon receiving Exit */
        if let Command::Exit = request.command {
            break;
        }

        /* Send back a response */
        let response = database::handle_request(request, &*db);

        stream.respond(&response)?;
        stream.flush()?;
    }

    Ok(())
}

