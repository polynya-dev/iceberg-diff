use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("schema mismatch: {0}")]
    SchemaMismatch(String),

    #[error("partition spec mismatch: {0}")]
    PartitionMismatch(String),

    #[error("primary key required: {0}")]
    MissingPrimaryKey(String),

    #[error(transparent)]
    Iceberg(#[from] iceberg::Error),

    #[error(transparent)]
    DataFusion(#[from] datafusion::error::DataFusionError),

    #[error(transparent)]
    Arrow(#[from] arrow::error::ArrowError),

    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error("{0}")]
    Other(String),
}

pub type Result<T> = std::result::Result<T, Error>;
