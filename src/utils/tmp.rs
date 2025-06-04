use std::{
    env::temp_dir,
    fs::{self, File}, io::BufReader,
};

use polars::{frame::DataFrame, io::{SerReader, SerWriter}, prelude::{CsvReader, CsvWriter, LazyCsvReader, LazyFrame}};
use rand::{distr::Alphanumeric, Rng};

pub fn rng_str(len: usize) -> String {
    rand::rng()
        .sample_iter(&Alphanumeric)
        .take(len)
        .map(char::from)
        .collect()
}

#[derive(Debug)]
pub struct TempFile {
    pub filepath: String,
}

impl TempFile {
    pub fn new(filepath: &str) -> TempFile {
        File::create(filepath).expect("fp create failed");
        TempFile { 
            filepath: filepath.to_owned()
        }
    }
    pub fn default_in_dir(dir: &str, ext: &str) -> TempFile {
        let rndstr = rng_str(12);
        let filepath = format!("{}/{}.{}", dir, &rndstr, ext);
        println!("temp filepath created: {}", &filepath);
        // sleep(time::Duration::from_secs(2000));
        TempFile::new(&filepath)
    }
    pub fn get(&self) -> Result<File, std::io::Error> {
        File::open(&self.filepath)
    }
    pub fn get_buf(&self) -> Result<BufReader<std::fs::File>, std::io::Error> {
        let f = File::open(&self.filepath)?;
        Ok(BufReader::new(f))
    }
    pub fn get_mut(&self) -> Result<File, std::io::Error> {
        File::create(&self.filepath)
    }
}

pub fn get_csv_frame(fp: &str) -> Result<DataFrame, std::io::Error> {
    let f = File::open(fp)?;
    let reader = CsvReader::new(f);
    Ok(reader.finish().expect("polars error"))
}


impl Default for TempFile {
    fn default() -> Self {
        let tmp_dir = temp_dir();
        let rndstr = rng_str(12);
        let filepath = format!("{}/{}", tmp_dir.to_str().unwrap(), &rndstr);
        TempFile::new(&filepath)
    }
}

impl Drop for TempFile {
    fn drop(&mut self) {
        fs::remove_file(&self.filepath).unwrap_or_else(|e| match e.kind() {
            std::io::ErrorKind::NotFound => {}
            other => panic!("Failed to delete TempFile {}: {:?}", &self.filepath, other),
        });
    }
}

