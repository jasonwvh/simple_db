use rocket::{get, post, routes, serde::json::Json, State};
use rocket::serde::{Deserialize, Serialize};
use libsimpledb::{Database, Table};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Mutex;

type ByteStr = [u8];
type ByteString = Vec<u8>;

#[derive(Deserialize)]
struct SetRequest {
    key: String,
    value: String
}

#[derive(Serialize)]
struct GetResponse {
    value: Option<String>,
}

#[derive(Serialize)]
struct ListResponse {
    keys: String,
    values: String,
}
fn store_index_on_disk(table: &mut libsimpledb::Table, index_key: &ByteStr) {
    table.index.remove(index_key);
    let index_as_bytes = bincode::serialize(&table.index).unwrap();
    table.index = std::collections::HashMap::new();
    table.insert(index_key, &index_as_bytes).unwrap();
}

#[get("/<table>")]
async fn list_handler(table: String, db: &State<Mutex<Database>>) -> Json<Vec<ListResponse>> {
    let table_path = Path::new(&table);
    let mut table = Table::open(table_path).expect("unable to open table");
    table.load().expect("unable to load data");

    let response: Vec<ListResponse> = table.list()
        .expect("unable to list data")
        .into_iter()
        .map(|kv| ListResponse {
            keys: String::from_utf8(kv.key).unwrap(),
            values: String::from_utf8(kv.value).unwrap(),
        })
        .collect();

    Json(response)
}

#[get("/<table>/<key>")]
async fn get_handler(table: String, key: String, db: &State<Mutex<Database>>) -> Json<GetResponse> {
    const INDEX_KEY: &ByteStr = b"+index";
    let mut db = db.lock().unwrap();
    let table_path = Path::new(&table);
    let mut table = Table::open(table_path).expect("unable to open table");
    table.load().expect("unable to load data");

    let index_as_bytes = table.get(&INDEX_KEY).unwrap().unwrap();
    let index_decoded = bincode::deserialize(&index_as_bytes);
    let index: HashMap<ByteString, u64> = index_decoded.unwrap();

    let response = match index.get(key.as_bytes()) {
        None => GetResponse { value: None },
        Some(&i) => {
            let kv = table.get_at(i).unwrap();
            GetResponse {
                value: Some(String::from_utf8(kv.value).unwrap()),
            }
        }
    };

    Json(response)
}

#[post("/<table>", data = "<req>")]
async fn set_handler(table: String, req: Json<SetRequest>, db: &State<Mutex<Database>>) -> &'static str {
    const INDEX_KEY: &ByteStr = b"+index";
    let mut db = db.lock().unwrap();
    db.create_table(table.as_str(), Path::new(&table)).expect("failed to create table");
    let table = db.get_table_mut(table.as_str()).expect("table not found");
    table.load().expect("unable to load data");

    table.insert(req.key.as_bytes(), req.value.as_bytes()).unwrap();
    store_index_on_disk(table, INDEX_KEY);

    "OK"
}

pub fn routes() -> Vec<rocket::Route> {
    routes![get_handler, list_handler, set_handler]
}