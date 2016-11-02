extern crate mqttc;
extern crate netopt;
extern crate postgres;
extern crate rustc_serialize;

use mqttc::PubSub;
use mqttc::ClientOptions;
use netopt::NetworkOptions;
use postgres::{Connection, TlsMode};
use rustc_serialize::json::Json;

fn main() {
    let netopt = NetworkOptions::new();

    let mut client = ClientOptions::new();

    let mut connection = client.connect("127.0.0.1:1883", netopt)
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

        save(database.to_string(), table.to_string(), schema.to_string(), json);
    }
}

fn save(database: String, table: String, schema: String, json: Json)
{
    let dsn = format!("postgres://postgres@localhost/{}", database);
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
