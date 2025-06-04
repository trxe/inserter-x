pub mod tmp;
use std::{io::{Read, Write}, process::exit};

use arrow::{array::{GenericByteViewArray, PrimitiveArray, StringViewArray}, ffi::from_ffi};
use polars::{df, io::{mmap::MmapBytesReader, SerWriter}, prelude::{CompatLevel, CsvWriter, DataFrame}};
use polars_arrow::{array::{BinaryArray, BinaryViewArrayGeneric, ListArray, Utf8Array}, ffi::{export_array_to_c, export_field_to_c}, io::ipc::format::ipc::Utf8View};
use postgres::NoTls;
use sqlx::QueryBuilder;
use tmp::TempFile;

pub struct TestPostgres {
    client: postgres::Client,
    tables: Vec<String>
}

impl TestPostgres {
    pub fn new(params: &str) -> Self {
        Self { client: postgres::Client::connect(params, NoTls).expect("client failed"), tables: vec![] }
    }

    pub async fn insert(&mut self, table: DataFrame) {
        let columns = table.take_columns();
        for mut column in columns {
            let name = column.name();
            let nname = name.to_owned();

            // let dtype = column.dtype();
            // let field = column.dtype().to_arrow_field(name.clone(), CompatLevel::newest());
            let series = column.into_materialized_series();
            // println!("{:?}", series.as_list().downcast_as_array().iter().collect::<Vec<_>>());
            let chunks = unsafe { series.chunks_mut() } ;
            println!("[{}] num chunks: {:?}\nchunks each: {:?}", nname, chunks.len(), chunks.iter().map(|x| x.len()).collect::<Vec<usize>>());


            chunks.iter_mut().for_each(|x| { 
                println!("dtype: {:?}", x.dtype());
                let utf8 = x.as_any().downcast_ref::<Utf8Array<i32>>();
                println!("utf8 array {:?}", utf8);
                let bina = x.as_any().downcast_ref::<BinaryArray<i32>>();
                println!("bina {:?}", bina);
                let list = x.as_any().downcast_ref::<ListArray<i32>>();
                println!("list {:?}", list);
                let bing = x.as_any().downcast_ref::<BinaryViewArrayGeneric<str>>().unwrap();
                let vv = bing.into_iter().map(|x| x.map(|y| y.to_owned())).collect::<Vec<_>>();
                println!("binary view array generic {:?}", vv);
                sqlx::query!(
    "
        INSERT INTO person2(name) 
        SELECT * FROM UNNEST($1::text[])
    ",
    &vv as &[Option<String>],
                );
                exit(0);
            });

            
            /*
            let arr = series.to_arrow(0, CompatLevel::newest());
            let v = unsafe {
                 std::mem::transmute::<polars_arrow::ffi::ArrowArray,arrow::ffi::FFI_ArrowArray>(export_array_to_c(arr))
            };
            let f = unsafe {
                 std::mem::transmute::<polars_arrow::ffi::ArrowSchema,arrow::ffi::FFI_ArrowSchema>(export_field_to_c(&field))
            };
            let final_values = unsafe {
                from_ffi(v, &f).unwrap()
            };
            println!("dtype: {}", final_values.data_type());
            let buffers = final_values.buffers();
            for buf in buffers {
                match *final_values.data_type() { 
                    arrow::datatypes::DataType::Int8 => { println!("{:?}", (buf.typed_data::<i8>())); },
                    arrow::datatypes::DataType::Int32 => { println!("{:?}", (buf.typed_data::<i32>())); },
                    arrow::datatypes::DataType::Utf8View => { 
                        println!("string {}: {:?}", nname, buf);
                    },
                    _ => {}
                }
            }
            */

        }
    }

    pub fn create(&mut self, columns: &str, table_name: &str) {
        let create = format!("CREATE TABLE IF NOT EXISTS {} (
            {}
        ); TRUNCATE {};", table_name, columns, table_name);
        println!("{}", create);
        self.client.batch_execute(&create).expect("failed table create");
        self.tables.push(table_name.to_owned());
    }

    pub fn fetch(&mut self, table_name: &str) -> Result<(), postgres::Error> {
        let query = format!("SELECT * FROM {}", table_name);
        let rows = self.client.query(&query, &[])?;
        for row in rows {
            println!("{:?}", row);
        }
        Ok(())
    }
}

impl Drop for TestPostgres {
    fn drop(&mut self) {
        let drops = self.tables.iter().map(|x| format!("DROP TABLE {};", x)).collect::<Vec<_>>();
        let drop = drops.join("\n");
        println!("Dropping:\n{}", drop);
        self.client.batch_execute(&drop).unwrap();
    }
}

pub struct TestData;

impl TestData {
    pub fn ohl25_top() -> DataFrame {
        df!(
            "first_name" => [ "Michael", "Ilya", "Liam", "Nick", "Zayne", ],
            "last_name" => [ "Misa", "Protas", "Greentree", "Lardis", "Parekh", ],
            "id" => [ 81, 82, 83, 84, 85 ],
            "pos" => [ "F", "F", "F", "F", "D" ],
            "goals" => [ 62, 50, 49, 71, 33 ],
            "assists" => [ 72, 74, 70, 46, 74 ],
            "test" => [ Some("yes"), None, Some("no"), None, Some("test") ],
        ).unwrap()
    }
}
