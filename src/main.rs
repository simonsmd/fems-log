use diesel::{insert_into, Connection, PgConnection, RunQueryDsl};
use tracing_subscriber::util::SubscriberInitExt;

use crate::fems::Fems;

mod fems;
mod schema;

type Error = anyhow::Error;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let filter = tracing_subscriber::EnvFilter::builder().from_env_lossy();
    tracing_subscriber::FmtSubscriber::builder()
        .with_env_filter(filter)
        .finish()
        .try_init()?;

    let database_url = "postgres://postgres@localhost/postgres";
    let mut connection = PgConnection::establish(database_url)
        .unwrap_or_else(|_| panic!("Error connecting to {}", database_url));


    let fems = Fems::connect("ws://192.168.178.125/websocket", "owner").await?;
    let mut channel = fems.subscribe_channels().await?;

    while let Some(data) = channel.recv().await {
        insert_into(schema::ems::dsl::ems).values(&data).execute(&mut connection)?;
    }

    Ok(())
}
