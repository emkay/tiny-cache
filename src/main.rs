use std::io;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream, Shutdown};
use std::str;
use std::time::{Duration, Instant};
use std::collections::HashMap;

fn main() -> io::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:11211")?;
    let mut db = HashMap::new();

    for stream in listener.incoming() {
        let stream = stream?;
        handle_client(stream, &mut db);
    }

    drop(listener);
    Ok(())
}

#[derive(Debug)]
struct Value {
    content: String,
    expires: Duration,
    timestamp: Instant
}

fn handle_client(mut stream: TcpStream, db: &mut HashMap<String, Value>) {
    let mut buffer = [0 as u8; 50];
    println!("Connection: {:?}", stream);
    loop {
        match stream.read(&mut buffer) {
            Ok(size) => {

                match str::from_utf8(&buffer[0..size]) {
                    Ok(line) => {
                        let actions: Vec<&str> = line.trim().split(" ").collect();
                        let length = actions.len();
                        let command = actions[0];

                        if command == "set" {
                            if length < 4 {
                                continue;
                            }

                            let key = String::from(actions[1]);
                            let content = String::from(actions[2]);
                            let expires_str = actions[3];

                            match expires_str.parse::<u64>() {
                                Ok(expires) => {
                                    let value = Value {
                                        content: content,
                                        expires: Duration::from_millis(expires),
                                        timestamp: Instant::now()
                                    };
                                    println!("key: {}", key);
                                    println!("value: {:?}", value);
                                    db.insert(key, value);
                                    break;
                                },
                                Err(_) => println!("An error occured parsing expires")
                            }
                        } else if command == "get" {
                            if length < 2 {
                                continue;
                            }

                            let key = String::from(actions[1]);
                            match db.get(&key) {
                                Some(value) => {
                                    let elapsed = value.timestamp.elapsed();
                                    println!("elapsed: {:?}", elapsed);
                                    match stream.write(value.content.as_bytes()) {
                                        Ok(_) => {
                                            match elapsed.checked_sub(value.expires) {
                                                Some(v) => {
                                                    println!("v: {:?}", v);
                                                    db.remove(&key);
                                                    break;
                                                },
                                                _ => break
                                            }
                                        },
                                        Err(_) => println!("an error occured writing")
                                    }
                                },
                                _ => {
                                    println!("No key");
                                    break;
                                }
                            }
                        } else if command == "delete" {
                            if length < 2 {
                                continue;
                            }

                            let key = String::from(actions[1]);
                            db.remove(&key);
                            break;
                        }
                    },
                    Err(_) => {
                        println!("An error occurred");
                    }
                }
            },
            Err(_) => {
                println!("An error occurred, terminating connection with {}", stream.peer_addr().unwrap());
                stream.shutdown(Shutdown::Both).unwrap();
            }
        }
    }
}
