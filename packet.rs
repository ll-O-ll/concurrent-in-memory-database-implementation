/*
 * packet.rs
 *
 * Implements packet sending and receiving
 *
 * University of Toronto
 * 2019
 */
 
use std::mem;
use std::io;

/* The foreign key is a reference to a row id in a separate table */
#[derive(Debug, PartialEq, PartialOrd, Clone)]
pub enum Value {
    Null,
    Integer(i64),
    Float(f64),
    Text(String),
    Foreign(i64),
}

impl Value {
    pub const NULL: i32 = 0;
    pub const INTEGER: i32 = 1;
    pub const FLOAT: i32 = 2; 
    pub const STRING: i32 = 3;
    pub const FOREIGN: i32 = 4; 
}

/* Specifies the 5 available commands in EasyDB */
#[derive(Debug)]
pub enum Command {
    Insert(Vec<Value>),            /* values */
    Update(i64, i64, Vec<Value>),  /* id, version, values */
    Drop(i64),                     /* id */
    Get(i64),                      /* id */
    Query(i32, i32, Value),        /* column_id, operator, value */
    Exit,                          /* disconnect from server */
}

#[derive(Debug)]
pub struct Request {
    pub table_id : i32,
    pub command : Command,
}

impl Request {
    pub const INSERT: i32 = 1;
    pub const UPDATE: i32 = 2;
    pub const DROP: i32 = 3;
    pub const GET: i32 = 4;   
    pub const SCAN: i32 = 5;
    pub const EXIT: i32 = 6;
}

/* we have to specify lifetime here because we are borrowing Vec<Value>
 * from a database row */
#[derive(Debug)]
pub enum Response {
    Error(i32),                 /* error code (except for OK) */
    Connected,
    Insert(i64, i64),           /* id, version */
    Update(i64),                /* version */
    Drop,
    Get(i64, Vec<Value>),   /* version, values */
    Query(Vec<i64>),            /* ids */
}

impl Response {
    pub const OK: i32 = 1;
    pub const NOT_FOUND: i32 = 2;       /* id not found */
    pub const BAD_TABLE: i32 = 3;       /* table not found */
    pub const BAD_QUERY: i32 = 4;       /* error during scan */ 
    pub const TXN_ABORT: i32 = 5;       /* transaction aborted */
    pub const BAD_VALUE: i32 = 6;       /* column value type mismatch */
    pub const BAD_ROW:   i32 = 7;       /* number of values is incorrect */
    pub const BAD_REQUEST: i32 = 8;     /* malformed packet */
    pub const BAD_FOREIGN: i32 = 9;     /* foreign key not found */
    pub const SERVER_BUSY: i32 = 10;    /* server is busy */
    pub const UNIMPLEMENTED: i32 = 11;  /* command not implemented */
}

/* trait for response packet (outgoing) */
trait Out<T: ?Sized> {   
    fn write(&mut self, value: &T);
}

trait Buffer {
    fn underfull(& self, size: usize) -> bool;
}

/* trait for request packet (incoming) */
trait In<T> : Buffer {
    fn read(&mut self) -> io::Result<T> {
        if self.underfull(self.size()) {
            Err(io::Error::new(io::ErrorKind::Other, "Incomplete packet"))
        }
        else {      
            Ok(self.from_raw())
        }
    }
    
    fn size(&self) -> usize;
    fn from_raw(&mut self) -> T;
}

/* buffer stores raw data for a packet */
struct ByteArray {
    buffer: Vec<u8>,
    pointer: usize,
    strlen: usize,
}

impl ByteArray {
    const MAX_PACKET_SIZE : usize = 16384;

    pub fn new() -> Self {
        ByteArray {
            buffer: vec!(),
            pointer: 0,
            strlen: 0,
        }
    }
    
    /* read the size field for the next value field to be read */
    fn read_size(& mut self) -> io::Result<i32> {
        let var: i32 = self.read()?;
        if var < 0 {
            return Err(io::Error::new(io::ErrorKind::Other,
                       "Read invalid value size"));
        }
        else {
            self.strlen = var as usize;
        }
        Ok(var)
    }
    
    /* size field is 8 bytes. next value field should be 8 bytes */
    fn read_fixed(& mut self) -> io::Result<i32> {
        let var: i32 = self.read_size()?;
        if var != mem::size_of::<i64>() as i32 {
            return Err(io::Error::new(io::ErrorKind::Other,
                       "Read invalid value size (fixed)"));
        }
        self.strlen = 0;
        Ok(var)
    }
    
    fn read_zero(& mut self) -> io::Result<i32> {
        let var: i32 = self.read_size()?;
        if var != 0 {
            return Err(io::Error::new(io::ErrorKind::Other,
                       "Read invalid value size (zero)"));
        }
        self.strlen = 0;
        Ok(var)
    }
    
    /* read size field followed by variant value field */
    fn read_value(&mut self) -> io::Result<Value> {
        let value_type: i32  = self.read()?;
        Ok(match value_type {
            Value::NULL => {
                self.read_zero()?;
                Value::Null
            },
            Value::INTEGER => {
                self.read_fixed()?;
                Value::Integer(self.read()?)
            },
            Value::FLOAT => {
                self.read_fixed()?;
                Value::Float(self.read()?)
            },
            Value::STRING => {
                self.read_size()?;
                Value::Text(self.read()?)
            },
            Value::FOREIGN => {
                self.read_fixed()?;
                Value::Foreign(self.read()?)
            },
            _ => {
                return Err(io::Error::new(io::ErrorKind::Other,
                                "Read invalid value type"));
            },
        })
    }
}

/* make sure we do not overflow buffer */
impl Buffer for ByteArray {
    fn underfull(& self, size: usize) -> bool {
        self.buffer.len() < self.pointer + size
    }
}

/* create packet from a byte array */
impl From<& [u8; ByteArray::MAX_PACKET_SIZE]> for ByteArray {
    fn from(buf: & [u8; ByteArray::MAX_PACKET_SIZE]) -> Self {
        ByteArray {
            buffer: buf.to_vec(),
            pointer: 0,
            strlen: 0,
        }
    }
}

impl Out<i32> for ByteArray {
    fn write(&mut self, value: &i32) {
        self.pointer += mem::size_of::<i32>();
        self.buffer.extend_from_slice(&value.to_be_bytes());
    }
}

impl In<i32> for ByteArray {
    fn size(&self) -> usize {
        mem::size_of::<i32>()
    }
    
    fn from_raw(&mut self) -> i32 {
        const SIZE: usize = mem::size_of::<i32>();
        
        let end = self.pointer + SIZE;
        let mut arr : [u8; SIZE] = [0; SIZE];

        arr.copy_from_slice(&self.buffer[self.pointer..end]);
        self.pointer = end;
        
        i32::from_be_bytes(arr)
    }
}

impl Out<i64> for ByteArray {
    fn write(&mut self, value: &i64) {
        self.pointer += mem::size_of::<i64>();
        self.buffer.extend_from_slice(&value.to_be_bytes());
    }
}

impl In<i64> for ByteArray {
    fn size(&self) -> usize {
        mem::size_of::<i64>()
    }
    
    fn from_raw(&mut self) -> i64 {
        const SIZE: usize = mem::size_of::<i64>();
        
        let end = self.pointer + SIZE;
        let mut arr : [u8; SIZE] = [0; SIZE];

        arr.copy_from_slice(&self.buffer[self.pointer..end]);
        self.pointer = end;
        
        i64::from_be_bytes(arr)
    }
}

impl Out<u64> for ByteArray {
    fn write(&mut self, value: &u64) {
        self.pointer += mem::size_of::<u64>();
        self.buffer.extend_from_slice(&value.to_be_bytes());
    }
}

impl Out<f64> for ByteArray {
    fn write(&mut self, value: &f64) {
        self.write(&value.to_bits());
    }
}

impl In<f64> for ByteArray {
    fn size(&self) -> usize {
        mem::size_of::<f64>()
    }
    
    fn from_raw(&mut self) -> f64 {
        const SIZE: usize = mem::size_of::<f64>();
        
        let end = self.pointer + SIZE;
        let mut arr : [u8; SIZE] = [0; SIZE];

        arr.copy_from_slice(&self.buffer[self.pointer..end]);
        self.pointer = end;
        
        f64::from_bits(u64::from_be_bytes(arr))
    }
}

// round up to nearest N bytes
fn aligned_size(orig: usize, align: usize) -> usize {
    ((orig + (align-1)) / align) * align
}

/* writing out a string */
impl Out<[u8]> for ByteArray {
    fn write(&mut self, value: &[u8]) {
        let size = aligned_size(value.len(), mem::size_of::<i32>());

        self.pointer += size;
        self.buffer.extend_from_slice(&value);
        
        // pad buffer with extra zeros
        for _ in 0..(size-value.len()) {
            self.buffer.push(0);
        }
    }
}

// for debugging only -- sending constant string
impl Out<str> for ByteArray {
    fn write(&mut self, value: &str) {
        self.write(value.as_bytes());
    }
}

impl Out<String> for ByteArray {
    fn write(&mut self, value: &String) {
        self.write(value.as_bytes());
    }
}

/* reading in a string */
impl In<String> for ByteArray {
    fn size(& self) -> usize {
        self.strlen
    }

    fn from_raw(&mut self) -> String {
        let end: usize = self.pointer + self.strlen;
        let s = String::from_utf8_lossy(&self.buffer[self.pointer..end]);
        self.pointer = end;
        self.strlen = 0;        /* consumed and reset */  
        let mut s = s.to_string();
        loop {
            if let Some(c) = s.pop() {
                if c != '\0' {
                    s.push(c);
                    break;
                }
            }
        }
        s
    }
}

pub trait Network : io::Write + io::Read {

    /* receive a packet from client */
    fn receive(&mut self) -> io::Result<Request> { 
        let mut buffer : [u8; ByteArray::MAX_PACKET_SIZE] = unsafe {
            mem::MaybeUninit::uninit().assume_init()
        };
      
        self.read(&mut buffer)?;
        let mut packet = ByteArray::from(&buffer);
        let cmd = packet.read()?;
        use self::Command::*;
       
        Ok(Request{ 
            table_id: packet.read()?, 
            command: match cmd {
                Request::INSERT => {
                    let mut vec = Vec::<Value>::new();
                    let numcols: i32 = packet.read()?;
                    
                    for _ in 0..numcols {
                        vec.push(packet.read_value()?);
                    }
                    
                    Insert(vec)
                },
                Request::UPDATE => {
                    let mut vec = Vec::<Value>::new();
                    let id: i64 = packet.read()?;
                    let version: i64 = packet.read()?;
                    let numcols: i32 = packet.read()?;
                    
                    for _ in 0..numcols {
                        vec.push(packet.read_value()?);
                    }
                    
                    Update(id, version, vec)
                },
                Request::DROP => {
                    Drop(packet.read()?)
                },
                Request::GET => {
                    Get(packet.read()?)
                },
                Request::SCAN => {
                    let column_id: i32 = packet.read()?;
                    let operator: i32 = packet.read()?;
                    Query(column_id, operator, packet.read_value()?)
                },
                Request::EXIT => Exit,
                _ => {
                    return Err(io::Error::new(io::ErrorKind::Other,
                                "Invalid command"));
                },
            },
        })
    }

    /* send packet to client */
    fn respond(&mut self, resp: &Response) -> io::Result<usize> {
        use self::Response::*;
        use self::Value::*;
        let mut packet = ByteArray::new();
        
        match resp {
            Error(code) => packet.write(code),
            Insert(id, version) => {
                packet.write(&Response::OK);
                packet.write(id);
                packet.write(version)
            },
            Update(version) => {
                packet.write(&Response::OK);
                packet.write(version)
            },
            Drop => packet.write(&Response::OK),
            Connected => packet.write(&Response::OK),
            Get(version, values) => {
                packet.write(&Response::OK);
                packet.write(version);
                packet.write(&(values.len() as i32));
                for value in values.into_iter() {
                    match value {
                        Null => {
                            packet.write(&Value::NULL);
                            packet.write(&(0 as i32));
                        },
                        Integer(v) => { 
                            packet.write(&Value::INTEGER);
                            packet.write(&(mem::size_of::<i64>() as i32));
                            packet.write(v);
                        },
                        Float(v) => { 
                            packet.write(&Value::FLOAT);
                            packet.write(&(mem::size_of::<f64>() as i32));
                            packet.write(v);
                        },
                        Text(v) => {
                            packet.write(&Value::STRING);
                            packet.write(&(aligned_size(v.len(), 
                                         mem::size_of::<i32>()) as i32));
                            packet.write(v);
                        },
                        Foreign(v) => { 
                            packet.write(&Value::FOREIGN);
                            packet.write(&(mem::size_of::<i64>() as i32));
                            packet.write(v);
                        },
                    };
                }
            },
            Query(ids) => {
                packet.write(&Response::OK);
                packet.write(&(ids.len() as i32));
                for id in ids {
                    packet.write(id);
                }
            },
        };
        
        self.write(&packet.buffer)
    }
}

