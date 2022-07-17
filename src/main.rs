use libsimpledb::Database;
use std::collections::HashMap;

#[cfg(target_os = "windows")]
const USAGE: &str = "
Usage:
    cargo run get KEY
    cargo run set KEY VALUE
";

type ByteStr = [u8];
type ByteString = Vec<u8>;

fn store_index_on_disk(a: &mut Database, index_key: &ByteStr) {
  a.index.remove(index_key);
  let index_as_bytes = bincode::serialize(&a.index).unwrap();
  a.index = std::collections::HashMap::new();
  a.insert(index_key, &index_as_bytes).unwrap();
}

fn main() {
  const INDEX_KEY: &ByteStr = b"+index";

  let args: Vec<String> = std::env::args().collect();
  let action = args.get(1).expect(&USAGE).as_ref();
  let key = args.get(2).expect(&USAGE).as_ref();
  let maybe_value = args.get(3);

  let path = std::path::Path::new("_store");
  let mut db = Database::open(path).expect("unable to open file");

  db.load().expect("unable to load data");

  match action {
    "get" => {
      let index_as_bytes = db.get(&INDEX_KEY)
                                    .unwrap()
                                    .unwrap();

      let index_decoded = bincode::deserialize(&index_as_bytes);

      let index: HashMap<ByteString, u64> = index_decoded.unwrap();

      match index.get(key) {
        None => eprintln!("{:?} not found", key),
        Some(&i) => {
          let kv = db.get_at(i).unwrap();
          println!("{:?}", kv.value)
        }
      }
    }

    "set" => {
      let value = maybe_value.expect(&USAGE).as_ref();
      db.insert(key, value).unwrap();
      store_index_on_disk(&mut db, INDEX_KEY);
    }

    _ => eprintln!("{}", &USAGE),
  }
}