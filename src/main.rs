use std::{fs::{self, DirEntry}, io, path::PathBuf};

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
    DownloadFolder {
        #[arg(short='s', long="src", value_name = "SOURCE")]
        source: String,
        #[arg(short='d', long="dest", value_name = "DESTINATION")]
        destination: PathBuf,
        #[arg(short='p', long="pattern", value_name = "PATTERN")]
        pattern: Option<String>,
    },
    List {
        #[arg(long, value_name = "FILE")]
        prefix: String,
        #[arg(short='p', long="pattern", value_name = "PATTERN")]
        pattern: Option<String>,
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
        Commands::DownloadFolder { source, destination, pattern } => {
            info!("Downloading folder: from {} to {}", source, destination.display());
            download_folder_command(&settings, &client, source, destination, pattern.as_deref()).await?;
        }
        Commands::List { prefix, pattern } => {
            info!("Listing files with prefix: {}", prefix);
            list_command(&settings, &client, prefix, pattern.as_deref()).await?;
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
	let mut destination: String = destination.to_string();
    if destination.starts_with("/") {
        return Err(anyhow!("Destination must not begin with /"));
    }
    if !destination.ends_with("/") {
        destination.push('/');
    }

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

    let dest = destination.as_os_str().to_str().ok_or(anyhow!("cannot get destination path {}", destination.display()))?;
    s3::download_large_file(&client, &settings.bucket_name, source, dest).await
}

async fn download_folder_command(
    settings: &settings::Settings,
    client: &aws_sdk_s3::Client,
    source: &str,
    destination: &PathBuf,
    pattern: Option<&str>,
) -> Result<(), anyhow::Error> {
    // Validation
    if source.starts_with("/") {
        return Err(anyhow!("Source must not begin with /"));
    }
    
    let mut source_prefix = source.to_string();
    // Only add trailing slash if source is not empty (empty = root folder)
    if !source_prefix.is_empty() && !source_prefix.ends_with("/") {
        source_prefix.push('/');
    }

    if destination.is_file() {
        return Err(anyhow!("Destination must be a directory"));
    }

    // Create destination directory if it doesn't exist
    if !destination.exists() {
        fs::create_dir_all(destination)?;
    }

    let dest_str = destination.as_os_str().to_str()
        .ok_or(anyhow!("Cannot get destination path {}", destination.display()))?;

    s3::download_folder_files(
        client,
        &settings.bucket_name,
        &source_prefix,
        dest_str,
        pattern,
    ).await
}

async fn list_command(settings: &settings::Settings, client: &aws_sdk_s3::Client, prefix: &str, pattern: Option<&str>) -> Result<(), anyhow::Error> {
    if prefix.starts_with("/") {
        return Err(anyhow!("Prefix must not begin with /"));
    }
    
    let mut folder_prefix = prefix.to_string();
    // Only add trailing slash if prefix is not empty (empty = root folder)
    if !folder_prefix.is_empty() && !folder_prefix.ends_with("/") {
        folder_prefix.push('/');
    }

    let list = s3::list_files(client, &settings.bucket_name, &folder_prefix, pattern).await?;
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

    s3::move_object(client, &settings.bucket_name, &source_key, &destination_key).await?;

    Ok(())
}
