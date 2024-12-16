#[macro_use] extern crate rocket;
mod api;

use rocket::State;
use std::sync::Mutex;
use libsimpledb::Database;

#[launch]
fn rocket() -> _ {
  let path = std::path::Path::new("_store");
  let db = Database::open(path).expect("unable to open file");

  rocket::build()
      .manage(Mutex::new(db))
      .mount("/", api::routes())
}