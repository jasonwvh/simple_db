use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io;
use std::io::prelude::*;
use std::io::{BufReader, BufWriter, SeekFrom};
use std::path::Path;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use serde_derive::{Deserialize, Serialize};

type ByteString = Vec<u8>;
type ByteStr = [u8];

#[derive(Debug, Serialize, Deserialize)] // #[derive(Debug)]
pub struct KeyValuePair {
  pub key: ByteString,
  pub value: ByteString,
}

#[derive(Debug)]
pub struct Table {
  f: File,
  pub index: HashMap<ByteString, u64>,
}

impl Table {
  pub fn open(path: &Path) -> io::Result<Self> {
    let f = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .append(true)
        .open(path)?;
    let index = HashMap::new();
    Ok(Table { f, index })
  }

  pub fn load(&mut self) -> io::Result<()> {
    let mut f = BufReader::new(&mut self.f);

    loop {
      let current_position = f.seek(SeekFrom::Current(0))?;

      let maybe_kv = Table::process_record(&mut f);
      let kv = match maybe_kv {
        Ok(kv) => kv,
        Err(err) => {
          match err.kind() {
            io::ErrorKind::UnexpectedEof => {
              break;
            }
            _ => return Err(err),
          }
        }
      };

      self.index.insert(kv.key, current_position);
    }

    Ok(())
  }

  fn process_record<R: Read>(f: &mut R) -> io::Result<KeyValuePair> {
    let key_len = f.read_u32::<LittleEndian>()?;
    let val_len = f.read_u32::<LittleEndian>()?;
    let data_len = key_len + val_len;

    let mut data = ByteString::with_capacity(data_len as usize);

    {
      f.by_ref().take(data_len as u64).read_to_end(&mut data)?;
    }
    debug_assert_eq!(data.len(), data_len as usize);

    let value = data.split_off(key_len as usize);
    let key = data;

    Ok(KeyValuePair { key, value })
  }

  pub fn list(&mut self) -> io::Result<Vec<KeyValuePair>> {
    let mut result = Vec::new();
    let positions: Vec<(ByteString, u64)> = self.index.iter().map(|(k, &v)| (k.clone(), v)).collect();
    for (key, position) in positions {
      if let Ok(kv) = self.get_at(position) {
        result.push(KeyValuePair { key, value: kv.value });
      }
    }
    Ok(result)
  }

  pub fn get(&mut self, key: &ByteStr) -> io::Result<Option<ByteString>> {
    let position = match self.index.get(key) {
      None => return Ok(None),
      Some(position) => *position,
    };

    let kv = self.get_at(position)?;

    Ok(Some(kv.value))
  }

  pub fn get_at(&mut self, position: u64) -> io::Result<KeyValuePair> {
    let mut f = BufReader::new(&mut self.f);
    f.seek(SeekFrom::Start(position))?;
    let kv = Table::process_record(&mut f)?;

    Ok(kv)
  }

  pub fn insert(
    &mut self,
    key: &ByteStr,
    value: &ByteStr,
  ) -> io::Result<()> {
    let mut f = BufWriter::new(&mut self.f);

    let key_len = key.len();
    let val_len = value.len();
    let mut tmp = ByteString::with_capacity(key_len + val_len);

    for byte in key {
      tmp.push(*byte);
    }

    for byte in value {
      tmp.push(*byte);
    }

    let next_byte = SeekFrom::End(0);
    let current_position = f.seek(SeekFrom::Current(0))?;
    f.seek(next_byte)?;
    f.write_u32::<LittleEndian>(key_len as u32)?;
    f.write_u32::<LittleEndian>(val_len as u32)?;
    f.write_all(&tmp)?;

    self.index.insert(key.to_vec(), current_position);
    Ok(())
  }
}

#[derive(Debug)]
pub struct Database {
  tables: HashMap<String, Table>,
}

impl Database {
  pub fn open(path: &Path) -> io::Result<Self> {
    let tables = HashMap::new();
    Ok(Database { tables })
  }

  pub fn create_table(&mut self, name: &str, path: &Path) -> io::Result<()> {
    let table = Table::open(path)?;
    self.tables.insert(name.to_string(), table);
    Ok(())
  }

  pub fn get_table(&self, name: &str) -> Option<&Table> {
    self.tables.get(name)
  }

  pub fn get_table_mut(&mut self, name: &str) -> Option<&mut Table> {
    self.tables.get_mut(name)
  }
}