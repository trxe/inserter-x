mod utils;
mod sources;
use std::sync::{atomic::AtomicBool, Arc};

use polars::prelude::*;
use argh::FromArgs;
use sources::pl_to_pg_type;
use utils::{tmp::{get_csv_frame, TempFile}, TestData};

#[derive(FromArgs)]
#[argh(description = "test")]
struct Args {
    #[argh(option, short = 'f', description = "fp")]
    pub filepath: String
}

#[tokio::main]
async fn main() {
    let args: Args = argh::from_env();
    let table = get_csv_frame(&args.filepath).expect("frame");

    let col_def = table.get_columns().iter().map(|x| {
        let pgtype = pl_to_pg_type(x);
        format!("{}\t\t{}", x.name(), pgtype)
    }).collect::<Vec<_>>().join(",\n");

    // let pool = sqlx::PgPool::connect("postgres://defuser:password@localhost:5432/defuser");

    println!("{}", col_def);
    let mut buffer = String::new();
    std::io::stdin().read_line(&mut buffer).expect("comma separated columns");
    let columns: Vec<PlSmallStr> = buffer.split(",").map(|x| x.trim().into()).collect();
    let actual_sel = table.select(columns).unwrap();

    let mut pg = utils::TestPostgres::new("host=localhost user=defuser password=password dbname=defuser");
    // let tmp = TempFile::default();
    // pg.create(&col_def, "exped");

    pg.insert(actual_sel).await;

    /*
    let alive = Arc::new(AtomicBool::new(true));
    let ctrlc_alive = alive.clone();

    ctrlc::set_handler(move || {
        println!("safely terminating...");
        ctrlc_alive.store(false, std::sync::atomic::Ordering::Relaxed);
    }).expect("Failed to setup ctrlc");

    while alive.load(std::sync::atomic::Ordering::Relaxed) {
        if let Err(e) = pg.fetch("exped") {
            println!("Error: {}", e);
            break;
        }
        std::thread::sleep(std::time::Duration::from_secs(10));
    }
    */

    println!("Bye!");
}
