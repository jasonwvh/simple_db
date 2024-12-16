use rocket::{get, post, routes, serde::json::Json, State};
use rocket::serde::{Deserialize, Serialize};
use libsimpledb::Database;
use std::collections::HashMap;
use std::sync::Mutex;

type ByteStr = [u8];
type ByteString = Vec<u8>;

#[derive(Deserialize)]
struct SetRequest {
    key: String,
    value: String,
}

#[derive(Serialize)]
struct GetResponse {
    value: Option<String>,
}

fn store_index_on_disk(a: &mut Database, index_key: &ByteStr) {
    a.index.remove(index_key);
    let index_as_bytes = bincode::serialize(&a.index).unwrap();
    a.index = std::collections::HashMap::new();
    a.insert(index_key, &index_as_bytes).unwrap();
}

#[get("/get/<key>")]
async fn get_handler(key: String, db: &State<Mutex<Database>>) -> Json<GetResponse> {
    const INDEX_KEY: &ByteStr = b"+index";
    let mut db = db.lock().unwrap();
    db.load().expect("unable to load data");

    let index_as_bytes = db.get(&INDEX_KEY).unwrap().unwrap();
    let index_decoded = bincode::deserialize(&index_as_bytes);
    let index: HashMap<ByteString, u64> = index_decoded.unwrap();

    let response = match index.get(key.as_bytes()) {
        None => GetResponse { value: None },
        Some(&i) => {
            let kv = db.get_at(i).unwrap();
            GetResponse {
                value: Some(String::from_utf8(kv.value).unwrap()),
            }
        }
    };

    Json(response)
}

#[post("/set", data = "<req>")]
async fn set_handler(req: Json<SetRequest>, db: &State<Mutex<Database>>) -> &'static str {
    const INDEX_KEY: &ByteStr = b"+index";
    let mut db = db.lock().unwrap();
    db.load().expect("unable to load data");

    db.insert(req.key.as_bytes(), req.value.as_bytes()).unwrap();
    store_index_on_disk(&mut db, INDEX_KEY);

    "OK"
}

pub fn routes() -> Vec<rocket::Route> {
    routes![get_handler, set_handler]
}