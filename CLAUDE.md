# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

`b2-rs` is a Rust CLI tool for interacting with S3-compatible object storage (Backblaze B2 and Huawei OBS). It wraps the AWS SDK for Rust with multipart upload/download support and a simple command interface.

## Commands

```bash
# Build
cargo build
cargo build --release

# Run
cargo run -- --config config.yaml <subcommand> [args]

# Check types without building
cargo check
```

### CLI subcommands (via `cargo run --`)

```bash
# Upload a single file to a prefix
cargo run -- upload -s <local-file> -d <prefix/>

# Upload all files in a directory
cargo run -- upload-folder -s <local-dir> -d <prefix/>

# Download a single object
cargo run -- download -s <object-key> -d <local-file>

# Download all objects under a prefix, with optional glob pattern
cargo run -- download-folder -s <prefix/> -d <local-dir> [-p "*.mp4"]

# List objects under a prefix, with optional glob pattern
cargo run -- list --prefix <prefix/> [-p "*.mp4"]

# Update an object's Content-Type (uses copy-in-place)
cargo run -- set-content-type -k <object-key> --content-type video/mp4

# Move (copy + delete) an object
cargo run -- move -s <source-key> -d <destination-key>
```

## Configuration

The tool reads a YAML config file (`config.yaml` by default, overridable with `--config`):

```yaml
endpoint: "https://s3.us-east-005.backblazeb2.com"
region: "us-east-005"
bucket_name: "my-bucket"
access_key: "..."
secret_key: "..."
```

Multiple per-bucket config files exist at the root (e.g., `kl026-etax-prod-obs-config.yaml`). Pass the appropriate one with `--config <file>`.

## Architecture

```
src/
  main.rs      — CLI parsing (clap derive), command dispatch, high-level command functions
  s3.rs        — All S3 operations: build_s3_client, upload, download, list, move, set-content-type
  settings.rs  — Loads config.yaml via the `config` crate into a Settings struct
  trace.rs     — Initializes tracing-subscriber with local-timezone timestamps
```

**Upload flow** (`s3::upload_large_file`): Uses S3 multipart upload with 5 MB chunks, up to 4 concurrent part uploads via `tokio::sync::Semaphore` and `JoinSet`. Parts are uploaded in any order and assembled by part number using a `BTreeMap`.

**Download flow** (`s3::download_large_file`): For small files (single chunk), uses a plain `GetObject`. For large files, pre-allocates the file on disk and fills it with range requests (4 concurrent), seeking to the correct offset before writing each chunk.

**`set-content-type`**: S3/B2 objects are immutable in metadata. This command performs a copy-to-self with `MetadataDirective::Replace` to update the Content-Type header.

**`move`**: Copy to destination key, then delete the source — S3 has no native move.

**Pattern matching** (`list`, `download-folder`): Uses `wildmatch` crate for glob-style patterns applied to the relative path within the prefix.
