extern crate mqttc;
extern crate netopt;
extern crate postgres;
extern crate rustc_serialize;

use mqttc::PubSub;
use mqttc::{Client, ClientOptions};
use netopt::NetworkOptions;
use postgres::{Connection, TlsMode};
use rustc_serialize::json::{self, Json};
use std::collections::HashMap;
use std::io::prelude::*;
use std::fs::File;

#[derive(RustcDecodable)]
pub struct ServerConfig {
    username: String,
    password: String,
    server: String,
    port: u16,
}

#[derive(RustcDecodable)]
pub struct Config  {
    mqtt: ServerConfig,
    postgresql: ServerConfig,
    schema: HashMap<String, String>,
}

fn main() {
    let config = match get_config() {
        Ok(config) => config,
        Err(err) => panic!("Unable to load configuration: {}", err),
    };

    let mut mqtt = match get_mqtt_connection(&config.mqtt) {
        Ok(mqtt) => mqtt,
        Err(err) => panic!("Unable to connect to mqtt server: {}", err),
    };

    mqtt.subscribe("#")
        .unwrap();

    loop {
        let message = match mqtt.await().unwrap() {
            Some(message) => message,
            None => continue,
        };

        let topic = message.topic.path.clone();
        let tokens: Vec<&str> = topic.split('/')
            .collect();

        let database = tokens[0];
        let table = tokens[1];

        let payload = match String::from_utf8((*message.payload).clone()) {
            Ok(payload) => payload,
            Err(_) => {
                println!("payload did not contain valid UTF-8");
                continue;
            },
        };

        let json = match Json::from_str(payload.as_str()) {
            Ok(json) => json,
            Err(err) => {
                println!("Invalid json payload: {}", err);
                continue;
            },
        };

        let schema = match config.schema.get(&topic) {
            Some(schema) => schema,
            None => {
                println!("Unknow topic: '{}'.", topic);
                continue;
            },
        };

        let pgsql = match get_pgsql_connection(&config.postgresql, database) {
            Ok(pgsql) => pgsql,
            Err(e) => {
                println!("Connection error: {:?}", e);
                return;
            },
        };

        save(pgsql, table, schema, json);
    }
}

fn get_config() -> Result<Config, String>
{
    let mut file = match File::open("config.json") {
        Ok(file) => file,
        Err(err) => return Err(format!("{}", err)),
    };

    let mut json = String::new();

    match file.read_to_string(&mut json) {
        Ok(_) => (),
        Err(err) => return Err(format!("{}", err)),
    };

    match json::decode(&json) {
        Ok(json) => Ok(json),
        Err(err) => return Err(format!("{}", err)),
    }
}

fn get_mqtt_connection(config: &ServerConfig) -> Result<Client, String>
{
    let netopt = NetworkOptions::new();

    let mut client = ClientOptions::new();
    client.set_username(config.username.clone())
        .set_password(config.password.clone());

    let dsn = format!("{}:{}", config.server, config.port);

    match client.connect(dsn.as_str(), netopt) {
        Ok(connection) => Ok(connection),
        Err(err) => Err(format!("{:?}", err)),
    }
}

fn get_pgsql_connection<T>(config: &ServerConfig, database: T) -> Result<Connection, String> where T: std::fmt::Display
{
    let dsn = format!("postgres://{}:{}@{}:{}/{}", config.username, config.password, config.server, config.port, database);

    match Connection::connect(dsn.as_str(), TlsMode::None) {
        Ok(connection) => Ok(connection),
        Err(err) => Err(format!("{:?}", err)),
    }
}

fn save<T, U>(connection: Connection, table: T, schema: U, json: Json) where T: std::fmt::Display, U: std::fmt::Display
{
    let sql = format!("INSERT INTO {} SELECT now() AS created, * FROM json_to_record($1) as x({})", table, schema);

    match connection.execute(sql.as_str(), &[&json]) {
        Ok(_) => (),
        Err(err) => println!("Unable to save invalid row: {}", err),
    };
}
