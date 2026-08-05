use std::io::{Cursor, Read, Write};

use anyhow::{Context, Result, bail, ensure};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use xet_core_structures::merklehash::{MerkleHash, compute_data_hash, file_hash, xorb_hash};
use xet_core_structures::metadata_shard::chunk_verification::range_hash_from_chunks;
use xet_core_structures::xorb_object::{
    RawXorbData, SerializedXorbObject, XorbObject, deserialize_chunk, deserialize_chunks,
};
use xet_data::deduplication::{Chunk, Chunker};
use xet_pkg::legacy::{Sha256Policy, XetFileInfo, data_client};
use xet_runtime::core::XetContext;

#[derive(Debug, Deserialize)]
struct TokenInfo {
    token: String,
    expiry: u64,
}

#[derive(Debug, Deserialize)]
struct UploadRequest {
    file_paths: Vec<String>,
    endpoint: String,
    token: Option<TokenInfo>,
    sha256s: Option<Vec<String>>,
    skip_sha256: bool,
}

#[derive(Debug, Deserialize)]
struct DownloadRequest {
    files: Vec<DownloadFile>,
    endpoint: String,
    token: Option<TokenInfo>,
}

#[derive(Debug, Deserialize)]
struct DownloadFile {
    destination_path: String,
    hash: String,
    file_size: i64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct ChunkInfo {
    hash: String,
    size: u64,
}

#[derive(Debug, Serialize)]
struct UploadResult {
    hash: String,
    file_size: u64,
    sha256: String,
}

fn main() {
    if std::env::var_os("XET_CORE_REFERENCE_DEBUG").is_some() {
        xet_pkg::init_logging("xet-core-reference".to_string());
    }
    if let Err(error) = run() {
        eprintln!("{error:#}");
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    let mut args = std::env::args().skip(1);
    let command = args.next().context("missing command")?;

    match command.as_str() {
        "chunk" => write_json(&chunk_stdin()?),
        "hash-chunk" => write_json(&hash_chunk_stdin()?),
        "hash-xorb" => write_json(&hash_list(HashKind::Xorb)?),
        "hash-file" => write_json(&hash_list(HashKind::File)?),
        "hash-range" => write_json(&hash_list(HashKind::Range)?),
        "hash-files" => {
            let paths: Vec<String> = read_json()?;
            let context = XetContext::default()?;
            let runtime = context.runtime.clone();
            let result = runtime.bridge_sync(async move {
                data_client::hash_files_async(&context, paths).await
            })??;
            write_json(&upload_results(result))
        }
        "upload-files" => {
            let request: UploadRequest = read_json()?;
            let policies = sha256_policies(&request)?;
            let token = request.token.map(|value| (value.token, value.expiry));
            let context = XetContext::default()?;
            let runtime = context.runtime.clone();
            let result = runtime.bridge_sync(async move {
                data_client::upload_async(
                    &context,
                    request.file_paths,
                    policies,
                    Some(request.endpoint),
                    token,
                    None,
                    None,
                    None,
                )
                .await
            })??;
            write_json(&upload_results(result))
        }
        "download-files" => {
            let request: DownloadRequest = read_json()?;
            let files = request
                .files
                .into_iter()
                .map(|value| {
                    let info = if value.file_size >= 0 {
                        XetFileInfo::new(value.hash, value.file_size as u64)
                    } else {
                        XetFileInfo::new_hash_only(value.hash)
                    };
                    (info, value.destination_path)
                })
                .collect();
            let token = request.token.map(|value| (value.token, value.expiry));
            let context = XetContext::default()?;
            let runtime = context.runtime.clone();
            let result = runtime.bridge_sync(async move {
                data_client::download_async(
                    &context,
                    files,
                    Some(request.endpoint),
                    token,
                    None,
                    None,
                    None,
                )
                .await
            })??;
            write_json(&result)
        }
        "encode-xorb" => {
            let with_footer = parse_bool_arg(args.next(), "with_footer")?;
            let compression = args.next().unwrap_or_else(|| "auto".to_string());
            encode_xorb(with_footer, &compression)
        }
        "decode-xorb" => {
            let with_footer = parse_bool_arg(args.next(), "with_footer")?;
            write_json(&decode_xorb(with_footer)?)
        }
        _ => bail!("unknown command {command:?}"),
    }
}

fn read_stdin() -> Result<Vec<u8>> {
    let mut input = Vec::new();
    std::io::stdin().read_to_end(&mut input)?;
    Ok(input)
}

fn read_json<T: DeserializeOwned>() -> Result<T> {
    serde_json::from_reader(std::io::stdin().lock()).context("invalid JSON request")
}

fn write_json<T: Serialize + ?Sized>(value: &T) -> Result<()> {
    serde_json::to_writer(std::io::stdout().lock(), value)?;
    Ok(())
}

fn chunk_data(data: &[u8]) -> Vec<Chunk> {
    let mut chunker = Chunker::default();
    let mut chunks = chunker.next_block(data, false);
    if let Some(chunk) = chunker.finish() {
        chunks.push(chunk);
    }
    chunks
}

fn chunk_stdin() -> Result<Vec<ChunkInfo>> {
    Ok(chunk_data(&read_stdin()?)
        .into_iter()
        .map(|chunk| ChunkInfo {
            hash: chunk.hash.hex(),
            size: chunk.data.len() as u64,
        })
        .collect())
}

fn hash_chunk_stdin() -> Result<String> {
    Ok(compute_data_hash(&read_stdin()?).hex())
}

enum HashKind {
    Xorb,
    File,
    Range,
}

fn hash_list(kind: HashKind) -> Result<String> {
    let chunks: Vec<ChunkInfo> = read_json()?;
    let chunks = chunks
        .into_iter()
        .map(|chunk| Ok((MerkleHash::from_hex(&chunk.hash)?, chunk.size)))
        .collect::<Result<Vec<_>>>()?;
    let hash = match kind {
        HashKind::Xorb => xorb_hash(&chunks),
        HashKind::File => file_hash(&chunks),
        HashKind::Range => {
            let hashes = chunks.into_iter().map(|(hash, _)| hash).collect::<Vec<_>>();
            range_hash_from_chunks(&hashes)
        }
    };
    Ok(hash.hex())
}

fn sha256_policies(request: &UploadRequest) -> Result<Vec<Sha256Policy>> {
    if request.skip_sha256 {
        ensure!(
            request.sha256s.is_none(),
            "sha256s and skip_sha256 are mutually exclusive"
        );
        return Ok(vec![Sha256Policy::Skip; request.file_paths.len()]);
    }

    let Some(sha256s) = &request.sha256s else {
        return Ok(vec![Sha256Policy::Compute; request.file_paths.len()]);
    };
    ensure!(
        sha256s.len() == request.file_paths.len(),
        "sha256 count does not match file count"
    );
    Ok(sha256s
        .iter()
        .map(|value| Sha256Policy::from_hex(value))
        .collect())
}

fn upload_results(values: Vec<XetFileInfo>) -> Vec<UploadResult> {
    values
        .into_iter()
        .map(|value| UploadResult {
            hash: value.hash().to_string(),
            file_size: value.file_size().unwrap_or(0),
            sha256: value.sha256().unwrap_or_default().to_string(),
        })
        .collect()
}

fn parse_bool_arg(value: Option<String>, name: &str) -> Result<bool> {
    match value.as_deref() {
        Some("true") => Ok(true),
        Some("false") => Ok(false),
        _ => bail!("{name} must be true or false"),
    }
}

fn encode_xorb(with_footer: bool, compression: &str) -> Result<()> {
    let chunks = chunk_data(&read_stdin()?);
    if chunks.is_empty() {
        ensure!(
            !with_footer,
            "xet-core does not serialize an empty xorb with a footer"
        );
        return Ok(());
    }

    let raw = RawXorbData::from_chunks(&chunks, vec![0]);
    let serialized = SerializedXorbObject::from_xorb(raw, with_footer, compression, 0)?;
    std::io::stdout().write_all(&serialized.serialized_data)?;
    Ok(())
}

fn decode_xorb(with_footer: bool) -> Result<Vec<ChunkInfo>> {
    let data = read_stdin()?;
    if !with_footer {
        let (raw, boundaries) = deserialize_chunks(&mut Cursor::new(data))?;
        return Ok(boundaries
            .windows(2)
            .map(|boundary| {
                let chunk = &raw[boundary[0] as usize..boundary[1] as usize];
                ChunkInfo {
                    hash: compute_data_hash(chunk).hex(),
                    size: chunk.len() as u64,
                }
            })
            .collect());
    }

    let mut reader = Cursor::new(data);
    let object = XorbObject::deserialize(&mut reader)?;
    let expected_hash = object.info.xorb_hash;
    ensure!(
        XorbObject::validate_xorb_object(&mut reader, &expected_hash)?.is_some(),
        "xorb validation failed"
    );

    let mut chunks = Vec::with_capacity(object.info.num_chunks as usize);
    let mut offset = 0_u64;
    for _ in 0..object.info.num_chunks {
        reader.set_position(offset);
        let (chunk, packed_size, _) = deserialize_chunk(&mut reader)?;
        chunks.push(ChunkInfo {
            hash: compute_data_hash(&chunk).hex(),
            size: chunk.len() as u64,
        });
        offset += packed_size as u64;
    }
    Ok(chunks)
}
