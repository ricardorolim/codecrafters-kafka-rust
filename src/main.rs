#![allow(unused_imports)]
use std::{
    io::Write,
    net::{TcpListener, TcpStream},
};

fn process(mut stream: TcpStream) {
    let message_size = 0 as u32;
    stream.write(&message_size.to_be_bytes()).unwrap();

    let correlation_id = 7 as u32;
    stream.write(&correlation_id.to_be_bytes()).unwrap();
}

fn main() {
    let listener = TcpListener::bind("127.0.0.1:9092").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                process(stream);
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
