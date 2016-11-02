extern crate mqttc;
extern crate netopt;
extern crate postgres;
extern crate rustc_serialize;

use mqttc::PubSub;
use mqttc::ClientOptions;
use netopt::NetworkOptions;
use postgres::{Connection, TlsMode};
use rustc_serialize::json::{self, Json};
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
    postgresql: ServerConfig
}

fn main() {
    let config = match get_config() {
        Ok(config) => config,
        Err(err) => panic!("Unable to load configuration: {}", err),
    };

    let netopt = NetworkOptions::new();

    let mut client = ClientOptions::new();
    client.set_username(config.mqtt.username)
        .set_password(config.mqtt.password);

    let dsn = format!("{}:{}", config.mqtt.server, config.mqtt.port);
    let mut connection = client.connect(dsn.as_str(), netopt)
        .expect("Can't connect to server");

    connection.subscribe("#")
        .unwrap();

    loop {
        let message = match connection.await().unwrap() {
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

        let schema = match topic.as_str() {
            "domotic/radiator" => "room_id int, radiator_id integer, powered boolean",
            "domotic/temperature" => "room_id int, temperature numeric",
            "domotic/humidity" => "room_id int, humidity numeric",
            "domotic/teleinfo" => "adco text, optarif text, isousc smallint, hchc bigint, hchp bigint, ptec text, iinst int, imax int, papp int, hhphc text, motdetat text",
            "domotic/vmc" => "speed integer, forced boolean",
            "domotic/weather" => "temperature_indoor numeric, temperature_outdoor numeric, dewpoint numeric, humidity_indoor integer, humidity_outdoor integer, wind_speed numeric, wind_dir numeric, wind_direction text, wind_chill numeric, rain_1h numeric, rain_24h numeric, rain_total numeric, pressure numeric, tendency text, forecast text",
            "network/connected_device" => "station_mac macaddr, ip inet, mac macaddr, virtual_mac macaddr",
            "system/log" => "level text, priority numeric, facility text, date timestamp, host text, message text, pid text, program text",
            _ => {
                println!("Unknow topic: '{}'.", topic);
                continue;
            },
        };

        save(&config.postgresql, database.to_string(), table.to_string(), schema.to_string(), json);
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

fn save(config: &ServerConfig, database: String, table: String, schema: String, json: Json)
{
    let dsn = format!("postgres://{}:{}@{}:{}/{}", config.username, config.password, config.server, config.port, database);
    let connection = match Connection::connect(dsn.as_str(), TlsMode::None) {
        Ok(connection) => connection,
        Err(e) => {
            println!("Connection error: {:?}", e);
            return;
        },
    };

    let sql = format!("INSERT INTO {} SELECT now() AS created, * FROM json_to_record($1) as x({})", table, schema);

    match connection.execute(sql.as_str(), &[&json]) {
        Ok(_) => (),
        Err(err) => println!("Unable to save invalid row: {}", err),
    };
}
