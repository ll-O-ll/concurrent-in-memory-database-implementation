/*
 * main.rs
 *
 * Processes arguments and starts EasyDB server
 *
 * University of Toronto
 * 2019
 */

mod schema;
mod packet;
mod server;
mod database;

use std::env;

fn usage(prog: &String) {
    println!("usage: {} [-g] PORT [FILE=default.txt] [HOST=localhost]", prog);
    println!("\t-g: debug mode (more verbose)");
    println!("\tFILE: EasyDB schema file");
    println!("\tHOST: host name");
}

fn main() {
    // Read in the arguments from command line
    let args: Vec<String> = env::args().collect();
    let mut hostname = String::from("localhost");
    let mut filename = String::from("default.txt");
    let mut verbose = false;
    
    if args.len() < 2 {
        return usage(&args[0]);
    }
    
    /* offset args by 1 depending on whether -g option is used */
    let args = match &args[1][..] {
        "-g" => { verbose = true; &args[1..] },
        _ => &args[..],
    };

    verbose = false;
    
    if args.len() < 2 || args.len() > 4 {
        return usage(&args[0]);
    }
       
    if args.len() >= 3 {
        filename.clear();
        filename.push_str(&args[2]);
    }
    
    if args.len() == 4 {
        hostname.clear();
        hostname.push_str(&args[3]);
    }
    
    hostname.push_str(":");
    hostname.push_str(&args[1]); 
    
    let tokens = match schema::tokenize(&filename) {
        Ok(tokens) => tokens,
        Err(e) => {
            eprintln!("Could not read from {}: {}", filename, e);
            return;
        },
    };

    let table_schema = match schema::parse(tokens) {
        Ok(table_schema) => table_schema,
        Err(e) => {
            eprintln!("Error processing {}: {}", filename, e);
            return;
        },
    };
   
    if verbose {
        println!("{:?}", table_schema);
    }
    
    server::run_server(table_schema, hostname, verbose);
}

