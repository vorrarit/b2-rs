use std::{collections::BTreeMap, fs, io::SeekFrom, path::Path, sync::Arc};

use anyhow::{Context, anyhow};
use aws_config::{Region, retry::RetryConfig};
use aws_credential_types::Credentials;
use aws_sdk_s3::{Client, config::{SharedCredentialsProvider, retry::ReconnectMode}, error::SdkError, primitives::ByteStream, types::{CompletedMultipartUpload, CompletedPart}};
use bytes::Bytes;
use http::{HeaderValue};
use mimetype_detector::{detect_file};
use sha2::{Digest, Sha256};
use tokio::{fs::{File, OpenOptions}, io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt}, sync::Semaphore, task::JoinSet};
use tracing::{error, info};
use wildmatch::WildMatch;

pub async fn build_s3_client(access_key: &str, secret_key: &str, region: &str, endpoint: &str) -> aws_sdk_s3::Client {
    let credentials = Credentials::from_keys(access_key, secret_key, None);
    let config = aws_config::from_env()
            .region(Region::new(region.to_string()))
            .credentials_provider(SharedCredentialsProvider::new(credentials))
            .endpoint_url(endpoint)
            .retry_config(RetryConfig::standard().with_reconnect_mode(ReconnectMode::ReuseAllConnections))
            .load()
            .await;

    let s3_config = aws_sdk_s3::config::Builder::from(&config)
        // .force_path_style(true)
        .build();

    let client = aws_sdk_s3::Client::from_conf(s3_config);
    client
}

const CHUNK_SIZE: usize = 1024 * 1024 * 5;
const MAX_CHUNKS: u64 = 10000;

struct UploadFileInfo {
    src_file: File,
    key: String,
    chunk_count: u64,
    mime_type: String
}

async fn get_upload_file_info(prefix: &str, source_filename: &str) -> Result<UploadFileInfo, anyhow::Error> {
    let source_meta = fs::metadata(source_filename)?;
    let file_size = source_meta.len();

    let mut chunk_count = (file_size / CHUNK_SIZE as u64) + 1;
    let mut _size_of_last_chunk = file_size % CHUNK_SIZE as u64;
    if _size_of_last_chunk == 0 {
        _size_of_last_chunk = CHUNK_SIZE as u64;
        chunk_count -= 1;
    }

    if file_size == 0 {
        return Err(anyhow!("Bad file size."));
    }
    if chunk_count > MAX_CHUNKS {
        return Err(anyhow!(
            "Too many chunks! Try increasing your chunk size.",
        ));
    }

    let path = Path::new(source_filename);

    let mime_type = detect_file(path)?;

    let filename = path.file_name()
            .ok_or(anyhow!("Cannot get filename string {}", source_filename))?
            .to_str()
            .ok_or(anyhow!("Cannot get filename string {}", source_filename))?;

    let key = if prefix.ends_with('/') {
        format!("{}{}", prefix, filename)
    } else {
        prefix.to_string()
    };

    let src_file = tokio::fs::File::open(path).await.expect("Failed to open file");

    Ok(UploadFileInfo {
        src_file,
        key,
        chunk_count,
        mime_type: mime_type.mime().to_string()
    })
}

// guarantee that byte read is exactly chunk_size or lesser than chunk_size in case of EOF
// error is returned only when there is an error reading the file, or when the chunk data is empty
async fn read_chunk(file: &mut File, chunk_size: usize) -> Result<(Bytes, usize), anyhow::Error> {
    let mut buffer = vec![0u8; chunk_size];
    let mut bytes_read = 0;
    while bytes_read < chunk_size {
        let n = file.read(&mut buffer[bytes_read..]).await?;
        if n == 0 { break; }
        bytes_read += n;
    }

    let chunk_data = Bytes::from(buffer[..bytes_read].to_vec());
    if chunk_data.is_empty() {
        return Err(anyhow!("Failed to read chunk data"));
    }
    let chunk_data_length = chunk_data.len();
    Ok((chunk_data, chunk_data_length))
}

pub async fn upload_large_file(client: &Client, bucket: &str, prefix: &str, source_filename: &str) -> Result<(), anyhow::Error> {
    let mut upload_file_info = get_upload_file_info(prefix, source_filename).await?;

    let multipart_upload_res = client.create_multipart_upload()
            .bucket(bucket)
            .key(&upload_file_info.key)
            .content_type(&upload_file_info.mime_type)
            .send()
            .await?;

    let upload_id = multipart_upload_res.upload_id()
            .ok_or(anyhow!("Missing upload_id after CreateMultipartUpload"))?;

    let mut part_number = 1;
    let semaphore = Arc::new(Semaphore::new(4));
    let mut set = JoinSet::new();

    loop {
        let (chunk_data, chunk_data_length) = read_chunk(&mut upload_file_info.src_file, CHUNK_SIZE).await?;

        let current_part = part_number;
        let client_clone = client.clone();
        let bucket_clone = bucket.to_string();
        let key_clone = upload_file_info.key.clone();
        let upload_id_clone = upload_id.to_string();

        let permit = Arc::clone(&semaphore).acquire_owned().await?;
        info!("Uploading part {}/{} : {} bytes", part_number, &upload_file_info.chunk_count, &chunk_data.len());

        set.spawn(async move {
            let _permit = permit;

            let mut hasher = Sha256::new();
            hasher.update(&chunk_data);
            let hash_hex = hex::encode(hasher.finalize());

            let upload_part_res = client_clone
                .upload_part()
                .bucket(bucket_clone)
                .key(key_clone)
                .upload_id(upload_id_clone)
                .body(ByteStream::from(chunk_data))
                .part_number(part_number)
                .customize()
                .mutate_request(move |req| {
                    req.headers_mut().insert(
                    "x-aws-content-sha256",
                        HeaderValue::from_str(&hash_hex).unwrap()
                    );
                })
                .send()
                .await;

            (current_part, upload_part_res)
        });

        part_number += 1;

        // exit loop if last chunk reached
        if chunk_data_length < CHUNK_SIZE { break; }

    }

    let mut completed_data = BTreeMap::new();

    while let Some(res) = set.join_next().await {
        match res {
            Ok((part, outcome)) => {
                match outcome {
                    Ok(upload_part_res) => {
                        let e_tag = upload_part_res.e_tag
                            .ok_or_else(|| anyhow!("Missing ETag in upload_part response for part {}", part))?;
                        completed_data.insert(part, e_tag);
                    }
                    Err(e) => {
                        return Err(anyhow!("Error uploading part {}: {}", part_number, e));
                    }
                }
            }
            Err(e) => {
                return Err(anyhow!("Task panicked or failed to join while uploading part {}: {}", part_number, e));
            }
        }
    }

    let completed_parts = completed_data.into_iter().map(|(part, e_tag)| {
            CompletedPart::builder()
                .e_tag(e_tag)
                .part_number(part)
                .build()
    }).collect::<Vec<CompletedPart>>();

    let completed_multipart_upload: CompletedMultipartUpload = CompletedMultipartUpload::builder()
            .set_parts(Some(completed_parts))
            .build();

    let _ = client
        .complete_multipart_upload()
        .bucket(bucket)
        .key(&upload_file_info.key)
        .multipart_upload(completed_multipart_upload)
        .upload_id(upload_id)
        .send()
        .await.context("Error during setting multipart upload as completed.")?;

    Ok(())
}

pub async fn download_large_file(client: &Client, bucket: &str, prefix: &str, destination_filename: &str) -> Result<(), anyhow::Error> {
    let head = client.head_object()
        .bucket(bucket)
        .key(prefix)
        .send()
        .await?;

    let file_size = head.content_length().ok_or(anyhow!("Fail to get file size."))? as u64;

    let mut chunk_count = (file_size / CHUNK_SIZE as u64) + 1;
    let mut _size_of_last_chunk = file_size % CHUNK_SIZE as u64;
    if _size_of_last_chunk == 0 {
        _size_of_last_chunk = CHUNK_SIZE as u64;
        chunk_count -= 1;
    }

    if file_size == 0 {
        return Err(anyhow!("Bad file size."));
    }
    if chunk_count > MAX_CHUNKS {
        return Err(anyhow!(
            "Too many chunks! Try increasing your chunk size.",
        ));
    }

    // If file can be downloaded in one chunk, download directly without range
    if chunk_count == 1 {
        info!("Downloading file in single request: {} bytes", file_size);
        
        let object_res = client.get_object()
            .bucket(bucket)
            .key(prefix)
            .send()
            .await
            .map_err(|e| anyhow!("Download failed: {:#?}", e))?;

        let data = object_res.body.collect().await.context("Failed to collect data.")?.into_bytes();

        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .open(destination_filename)
            .await?;
        file.write_all(&data).await.context("Failed to write data.")?;

        return Ok(());
    }

    // For large files, use range requests
    let file = OpenOptions::new()
        .write(true)
        .create(true)
        .open(destination_filename)
        .await?;
    file.set_len(file_size).await?;
    let shared_file = Arc::new(tokio::sync::Mutex::new(file));


    let semaphore = Arc::new(Semaphore::new(4));
    let mut set = JoinSet::new();

    let mut start = 0;
    let mut part_number = 1;
    while start < file_size {
        let end = std::cmp::min(start + CHUNK_SIZE as u64 - 1, file_size - 1);
        let range = format!("bytes={}-{}", start, end);

        let permit = Arc::clone(&semaphore).acquire_owned().await?;
        let client_clone = client.clone();
        let bucket_clone = bucket.to_string();
        let prefix_clone = prefix.to_string();
        let file_ref = Arc::clone(&shared_file);

        info!("Downloading part {}/{} : {} bytes", part_number, chunk_count, end-start);

        set.spawn(async move {
            let _permit = permit;
            let object_res = client_clone.get_object()
                .bucket(bucket_clone)
                .key(prefix_clone)
                .range(range.clone())
                .send()
                .await
                .map_err(|e| anyhow!("Download failed for range {}: {}", range, e.into_service_error()))?;

            let data = object_res.body.collect().await.context("Failed to collect data.")?.into_bytes();

            // write file
            let mut file_guard = file_ref.lock().await;
            file_guard.seek(SeekFrom::Start(start)).await.context("Failed to seek file.")?;
            file_guard.write_all(&data).await.context("Failed to write data.")?;

            Ok::<(), anyhow::Error>(())
        });

        start += CHUNK_SIZE as u64;
        part_number += 1;
    }

    while let Some(res) = set.join_next().await {
        res.context("Task panicked or failed to join")??;
    }

    Ok(())
}

pub async fn download_folder_files(
    client: &Client,
    bucket: &str,
    prefix: &str,
    destination_dir: &str,
    pattern: Option<&str>,
) -> Result<(), anyhow::Error> {
    // List all objects with the given prefix (filtering already done in list_files)
    let all_objects = list_files(client, bucket, prefix, pattern).await?;
    
    info!("Found {} files to download", all_objects.len());
    
    let mut files_to_download = Vec::new();
    for key in all_objects {
        let relative_path = key.strip_prefix(prefix).unwrap_or(&key);
        files_to_download.push((key.clone(), relative_path.to_string()));
    }
    
    // Download files concurrently with a limit of 4
    let semaphore = Arc::new(Semaphore::new(4));
    let mut set = JoinSet::new();
    
    for (key, filename) in files_to_download {
        let dest_path = Path::new(destination_dir).join(&filename);
        let dest_str = dest_path.to_string_lossy().to_string();
        
        let permit = Arc::clone(&semaphore).acquire_owned().await?;
        let client_clone = client.clone();
        let bucket_clone = bucket.to_string();
        
        info!("Downloading {} to {}", key, dest_str);
        
        set.spawn(async move {
            let _permit = permit;
            
            match download_large_file(&client_clone, &bucket_clone, &key, &dest_str).await {
                Ok(_) => {
                    info!("Successfully downloaded {}", key);
                    Ok(())
                }
                Err(e) => {
                    error!("Failed to download {}: {}", key, e);
                    // Return error but we'll handle it to continue with other files
                    Err(e)
                }
            }
        });
    }
    
    // Wait for all downloads to complete, continuing on individual errors
    while let Some(res) = set.join_next().await {
        match res {
            Ok(download_result) => {
                // Ignore individual download failures, just log them
                let _ = download_result;
            }
            Err(e) => {
                error!("Task panicked or failed to join: {}", e);
            }
        }
    }
    
    Ok(())
}

pub async fn is_file_exists(client: &Client, bucket: &str, prefix: &str) -> Result<bool, anyhow::Error> {
    let res = client.head_object()
            .bucket(bucket)
            .key(prefix)
            .send()
            .await;

    match res {
        Ok(_) => Ok(true),
        Err(SdkError::ServiceError(err)) => {
            if err.raw().status().as_u16() == 404 {
                Ok(false)
            } else {
                Err(anyhow!("error {:?}", err))
            }
        },
        Err(e) => {
            Err(anyhow!("error {:#} ", e))
        }
    }
}

pub async fn list_files(client: &Client, bucket: &str, prefix: &str, pattern: Option<&str>) -> Result<Vec<String>, anyhow::Error> {
    let mut list = Vec::new();
    let matcher = pattern.map(|p| WildMatch::new(p));

    let mut response = client.list_objects_v2()
        .bucket(bucket)
        .prefix(prefix)
        .max_keys(10)
        .delimiter("/")
        .into_paginator()
        .send();

    while let Some(result) = response.next().await {
        match result {
            Ok(output) => {
                // List files in current folder
                for object in output.contents() {
                    let key = object.key().unwrap_or_default();
                    
                    // Get relative path from prefix
                    let relative_path = key.strip_prefix(prefix).unwrap_or(key);
                    
                    // Skip if empty
                    if relative_path.is_empty() {
                        continue;
                    }
                    
                    // Apply pattern matching if provided
                    if let Some(ref m) = matcher {
                        if !m.matches(relative_path) {
                            continue;
                        }
                    }
                    
                    list.push(key.to_string());
                }
                
                // List subfolders (common prefixes)
                for common_prefix in output.common_prefixes() {
                    if let Some(folder_key) = common_prefix.prefix() {
                        let relative_path = folder_key.strip_prefix(prefix).unwrap_or(folder_key);
                        
                        // Skip if empty
                        if relative_path.is_empty() {
                            continue;
                        }
                        
                        // Remove trailing slash for pattern matching
                        let folder_name = relative_path.trim_end_matches('/');
                        
                        // Apply pattern matching if provided
                        if let Some(ref m) = matcher {
                            if !m.matches(folder_name) {
                                continue;
                            }
                        }
                        
                        list.push(folder_key.to_string());
                    }
                }
            },
            Err(err) => {
                return Err(anyhow!("error {:?}", err));
            }
        }
    }
    Ok(list)
}

pub async fn update_object_content_type(
    client: &Client,
    bucket: &str,
    key: &str,
    content_type: &str,
) -> Result<(), anyhow::Error> {
    // Construct the copy source string (e.g., "/your-bucket-name/your-object-key")
    let copy_source = format!("/{}/{}", bucket, key);

    client
        .copy_object()
        .copy_source(copy_source)
        .bucket(bucket)
        .key(key)
        .content_type(content_type) // Set the new content type
        .metadata_directive(aws_sdk_s3::types::MetadataDirective::Replace) // Crucial: tells S3 to replace all metadata with the new values
        .send()
        .await?;

    Ok(())
}

pub async fn move_object(
    client: &Client,
    bucket: &str,
    source_key: &str,
    destination_key: &str,
) -> Result<(), anyhow::Error> {
    // Construct the copy source string (e.g., "/your-bucket-name/your-object-key")
    let copy_source = format!("/{}/{}", bucket, source_key);

    client
        .copy_object()
        .copy_source(copy_source)
        .bucket(bucket)
        .key(destination_key)
        .send()
        .await?;

    client
        .delete_object()
        .bucket(bucket)
        .key(source_key)
        .send()
        .await?;

    Ok(())
}
