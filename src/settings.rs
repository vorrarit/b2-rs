use std::{path::PathBuf};

use serde::Deserialize;
use config::{Config, ConfigError, File};

#[derive(Debug, Deserialize)]
pub struct Settings {
    pub endpoint: String,
    pub region: String,
    pub bucket_name: String,
    pub access_key: String,
    pub secret_key: String,
}

impl Settings {
    pub fn new(param_config: Option<&PathBuf>) -> Result<Self, ConfigError> {
        let config_filepath = match param_config {
            Some(c) => c.display().to_string(),
            None => "config.yaml".to_string(),
        };

        let s = Config::builder()
            .add_source(File::with_name(&config_filepath))
            .build()?;

        s.try_deserialize()
    }
}
