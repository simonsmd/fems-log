use anyhow::Context;
use diesel::{Connection, PgConnection, RunQueryDsl, insert_into};
use diesel_migrations::{EmbeddedMigrations, MigrationHarness, embed_migrations};
use tracing::error;
use tracing::level_filters::LevelFilter;
use tracing_subscriber::util::SubscriberInitExt;

use crate::fems::Fems;

mod fems;
mod schema;

type Error = anyhow::Error;

pub const MIGRATIONS: EmbeddedMigrations = embed_migrations!("migrations");

#[tokio::main]
async fn main() -> Result<(), Error> {
    let filter = tracing_subscriber::EnvFilter::builder()
        .with_default_directive(LevelFilter::INFO.into())
        .from_env()?;
    tracing_subscriber::FmtSubscriber::builder()
        .with_env_filter(filter)
        .finish()
        .try_init()?;

    let database_url = std::env::var("DATABASE_URL").context("DATABASE_URL not set")?;
    let mut connection =
        PgConnection::establish(&database_url).context("database connection failed")?;

    connection
        .run_pending_migrations(MIGRATIONS)
        .map_err(|e| anyhow::anyhow!(e))
        .context("failed to run migrations")?;

    let fems_url = std::env::var("FEMS_URL").context("FEMS_URL not set")?;
    let fems_password = std::env::var("FEMS_PASSWORD").context("FEMS_PASSWORD not set")?;
    let fems = Fems::connect(&fems_url, &fems_password).await?;
    let mut channel = fems.subscribe_channels().await?;

    while let Some(data) = channel.recv().await {
        if let Err(error) = insert_into(schema::ems::dsl::ems)
            .values(&data)
            .execute(&mut connection)
        {
            error!("Error while inserting data into db: {error:?}");
        }
    }

    Ok(())
}
