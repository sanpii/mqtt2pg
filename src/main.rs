extern crate app_dirs;
extern crate mqttc;
extern crate netopt;
extern crate postgres;
extern crate rustc_serialize;

use app_dirs::{AppInfo, AppDataType};
use mqttc::PubSub;
use mqttc::{Client, ClientOptions};
use netopt::NetworkOptions;
use postgres::{Connection, TlsMode};
use rustc_serialize::json::{self, Json};
use std::collections::HashMap;
use std::io::prelude::*;
use std::fs::File;
use std::sync::Arc;
use std::thread;

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

const APP_INFO: AppInfo = AppInfo { name: "mqtt2pg", author: "Sanpi" };

fn main()
{
    let mut threads = vec![];

    let config = match get_config() {
        Ok(config) => Arc::new(config),
        Err(err) => panic!("Unable to load configuration: {}", err),
    };

    let databases = get_databases(&config.schema);

    for database in databases {
        let config = config.clone();

        let thread = thread::spawn(move || {
            let topic = format!("{}/#", database);

            let mut mqtt = match get_mqtt_connection(&config.mqtt) {
                Ok(mqtt) => mqtt,
                Err(err) => panic!("Unable to connect to mqtt server: {}", err),
            };

            let pgsql = match get_pgsql_connection(&config.postgresql, database.as_str()) {
                Ok(pgsql) => pgsql,
                Err(e) => panic!("Unable to connect to pgsql server: {:?}", e),
            };

            listen(&mut mqtt, pgsql, &config, topic);
        });

        threads.push(thread);
    }

    for thread in threads {
        let _ = thread.join();
    }
}

fn get_config() -> Result<Config, String>
{
    let mut config = match app_dirs::get_app_root(AppDataType::UserConfig, &APP_INFO) {
        Ok(config_dir) => config_dir,
        Err(err) => return Err(format!("{}", err)),
    };
    config.push("config.json");

    let mut file = match File::open(config) {
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

fn get_databases(schemas: &HashMap<String, String>) -> Vec<String>
{
    let mut databases = Vec::new();

    for topic in schemas.keys() {
        let tokens: Vec<&str> = topic.split('/').collect();
        let database = tokens[0].to_string();

        if !databases.contains(&database) {
            databases.push(database);
        }
    }

    databases
}

fn listen(mqtt: &mut Client, pgsql: Connection, config: &Config, topic: String)
{
    mqtt.subscribe(topic.as_str()).unwrap();

    loop {
        let message = match mqtt.await().unwrap() {
            Some(message) => message,
            None => continue,
        };

        let topic = message.topic.path.clone();
        let tokens: Vec<&str> = topic.split('/')
            .collect();

        let table = tokens[1];

        let payload = match String::from_utf8((*message.payload).clone()) {
            Ok(payload) => payload,
            Err(_) => {
                println!("payload did not contain valid UTF-8");
                continue;
            },
        };

        let schema = match config.schema.get(&topic) {
            Some(schema) => schema,
            None => panic!("Unknow topic: '{}'.", topic),
        };

        let json = match Json::from_str(payload.as_str()) {
            Ok(json) => json,
            Err(err) => {
                println!("Invalid json payload: {} [{}]", err, payload);
                continue;
            },
        };

        save(&pgsql, table, schema, json);
    }
}

fn get_pgsql_connection(config: &ServerConfig, database: &str) -> Result<Connection, String>
{
    let dsn = format!("postgres://{}:{}@{}:{}/{}", config.username, config.password, config.server, config.port, database);

    match Connection::connect(dsn.as_str(), TlsMode::None) {
        Ok(connection) => Ok(connection),
        Err(err) => return Err(format!("{:?}", err)),
    }
}

fn save<T, U>(connection: &Connection, table: T, schema: U, json: Json) where T: std::fmt::Display, U: std::fmt::Display
{
    let sql = format!("INSERT INTO {} SELECT now() AS created, * FROM json_to_record($1) as x({})", table, schema);

    match connection.execute(sql.as_str(), &[&json]) {
        Ok(_) => (),
        Err(err) => println!("Unable to save invalid row: {}", err),
    };
}
