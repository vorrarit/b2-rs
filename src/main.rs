use std::{error::Error, fs::{self, DirEntry}, io, path::PathBuf};

use clap::{Parser, Subcommand};
use anyhow::anyhow;
use tracing::{error, info};

mod settings;
mod trace;
mod s3;

#[derive(Parser, Debug)]
struct Cli {
    #[arg(short, long, value_name = "FILE", global = true, default_value = "config.yaml")]
    config: Option<PathBuf>,

    #[command(subcommand)]
    command: Commands
}

#[derive(Debug, Subcommand)]
enum Commands {
    Upload {
        #[arg(short='s', long="src", value_name = "SOURCE")]
        source: PathBuf,
        #[arg(short='d', long="dest", value_name = "DESTINATION")]
        destination: String,
    },
    UploadFolder {
        #[arg(short='s', long="src", value_name = "SOURCE")]
        source: PathBuf,
        #[arg(short='d', long="dest", value_name = "DESTINATION")]
        destination: String,
    },
    Download {
        #[arg(short='s', long="src", value_name = "SOURCE")]
        source: String,
        #[arg(short='d', long="dest", value_name = "DESTINATION")]
        destination: PathBuf,
    },
    List {
        #[arg(short, long, value_name = "FILE")]
        prefix: String,
    },
    SetContentType {
        #[arg(short='k', long, value_name = "FILE")]
        key: String,
        #[arg(long="content-type", value_name = "CONTENT_TYPE")]
        content_type: String,
    },
    Move {
        #[arg(short='s', long, value_name = "FILE")]
        source_key: String,
        #[arg(short='d', long, value_name = "FILE")]
        destination_key: String
    }
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    println!("Hello, world!");


    trace::trace_init();

    let cli = Cli::parse();

    let settings = settings::Settings::new(cli.config.as_ref())?;
    let client = s3::build_s3_client(
        &settings.access_key,
        &settings.secret_key,
        &settings.region,
        &settings.endpoint).await;


    match &cli.command {
        Commands::Upload { source , destination} => {
            info!("Uploading file: from {} to {}", source.display(), destination);
            upload_command(&settings, &client, source, &destination).await?;
        }
        Commands::UploadFolder { source, destination } => {
            info!("Uploading folder: from {} to {}", source.display(), destination);
            upload_folder_command(&settings, &client, source, &destination).await?;
        }
        Commands::Download { source, destination } => {
            info!("Downloading file: from {} to {}", source, destination.display());
            download_command(&settings, &client, source, destination).await?;
        }
        Commands::List { prefix } => {
            info!("Listing files with prefix: {}", prefix);
            list_command(&settings, &client, prefix).await?;
        }
        Commands::SetContentType { key, content_type } => {
            info!("Set Content Type: key {} content-type {}", key, content_type);
            set_content_type_command(&settings, &client, key, content_type).await?;
        }
        Commands::Move { source_key, destination_key } => {
            info!("Moveing Object: from {} to {}", source_key, destination_key);
            move_object_command(&settings, &client, source_key, destination_key).await?;
        }
    }

    Ok(())
}

async fn upload_command(settings: &settings::Settings, client: &aws_sdk_s3::Client, source: &PathBuf, destination: &str) -> Result<(), anyhow::Error> {
 //   	TS_UAT_ACCESS_KEY := "00507fad99e41ff0000000002"
	// TS_UAT_SECRET := "K005K8xo6A5znxM9bW7TyoOleRudCJo"
	// TS_UAT_ENDPOINT := "https://s3.us-east-005.backblazeb2.com"
	// TS_UAT_BUCKET := "vorrarit-demo-bucket"
	// TS_UAT_LIST_PATH := "data/"

	let mut destination: String = destination.to_string();
    if destination.starts_with("/") {
        return Err(anyhow!("Destination must not begin with /"));
    }
    if !destination.ends_with("/") {
        destination.push('/');
    }

    // let client = s3::build_s3_client(
    //     "00507fad99e41ff0000000002",
    //     "K005K8xo6A5znxM9bW7TyoOleRudCJo",
    //     "ap-southeast-2",
    //     "https://s3.us-east-005.backblazeb2.com").await;
    let src = source.as_os_str().to_str().ok_or(anyhow!("cannot get source path {}", source.display()))?;
    s3::upload_large_file(&client, &settings.bucket_name, &destination, src).await
}

async fn upload_folder_command(settings: &settings::Settings, client: &aws_sdk_s3::Client, source: &PathBuf, destination: &str) -> Result<(), anyhow::Error> {
    if source.is_file() {
        return Err(anyhow!("Source must be a directory"));
    }
    let mut destination: String = destination.to_string();
    if destination.starts_with("/") {
        return Err(anyhow!("Destination must not begin with /"));
    }
    if !destination.ends_with("/") {
        destination.push('/');
    }

    let mut entries = fs::read_dir(source)?.collect::<Result<Vec<DirEntry>, io::Error>>()?;
    entries.sort_by_key(|entry| entry.file_name());
    for entry in entries {
        let file_path = entry.path();
        info!("Name: {}", file_path.display());
        upload_command(settings, client, &file_path, &destination).await.unwrap_or_else(|err| {
            error!("Failed to upload file {}: {}", file_path.display(), err);
        });
    }

    Ok(())
}

async fn download_command(settings: &settings::Settings, client: &aws_sdk_s3::Client, source: &str, destination: &PathBuf) -> Result<(), anyhow::Error> {

    if source.starts_with("/") {
        return Err(anyhow!("Source must not begin with /"));
    }

    // let client = s3::build_s3_client(
    //     "00507fad99e41ff0000000002",
    //     "K005K8xo6A5znxM9bW7TyoOleRudCJo",
    //     "ap-southeast-2",
    //     "https://s3.us-east-005.backblazeb2.com").await;

    let dest = destination.as_os_str().to_str().ok_or(anyhow!("cannot get destination path {}", destination.display()))?;
    s3::download_large_file(&client, &settings.bucket_name, source, dest).await
}

async fn list_command(settings: &settings::Settings, client: &aws_sdk_s3::Client, prefix: &str) -> Result<(), anyhow::Error> {
    if prefix.starts_with("/") {
        return Err(anyhow!("Prefix must not begin with /"));
    }

    // let client = s3::build_s3_client(
    //     "00507fad99e41ff0000000002",
    //     "K005K8xo6A5znxM9bW7TyoOleRudCJo",
    //     "ap-southeast-2",
    //     "https://s3.us-east-005.backblazeb2.com").await;

    let list = s3::list_files(client, &settings.bucket_name, prefix).await?;
    info!("Files with prefix '{}':", prefix);
    for file in list {
        info!("{}", file);
    }

    Ok(())
}

async fn set_content_type_command(settings: &settings::Settings, client: &aws_sdk_s3::Client, key: &str, content_type: &str) -> Result<(), anyhow::Error> {
    if key.starts_with("/") {
        return Err(anyhow!("Key must not begin with /"));
    }

    // let client = s3::build_s3_client(
    //     "00507fad99e41ff0000000002",
    //     "K005K8xo6A5znxM9bW7TyoOleRudCJo",
    //     "ap-southeast-2",
    //     "https://s3.us-east-005.backblazeb2.com").await;

    s3::update_object_content_type(client, &settings.bucket_name, key, content_type).await?;

    Ok(())
}

async fn move_object_command(settings: &settings::Settings, client: &aws_sdk_s3::Client, source_key: &str, destination_key: &str) -> Result<(), anyhow::Error> {
    if source_key.starts_with("/") {
        return Err(anyhow!("Source key must not begin with /"));
    }
    if destination_key.starts_with("/") {
        return Err(anyhow!("Destination key must not begin with /"));
    }

    // let client = s3::build_s3_client(
    //     "00507fad99e41ff0000000002",
    //     "K005K8xo6A5znxM9bW7TyoOleRudCJo",
    //     "ap-southeast-2",
    //     "https://s3.us-east-005.backblazeb2.com").await;

    s3::move_object(client, &settings.bucket_name, &source_key, &destination_key).await?;

    Ok(())
}
