//! Fixed-flow PubChem topology pipeline with optional Zenodo publication.
//!
//! The library exists mainly to support the single binary and its tests, while
//! keeping the actual compute and publish logic reusable from integration code.
//!
#![cfg_attr(
    test,
    allow(
        clippy::expect_used,
        clippy::missing_errors_doc,
        clippy::missing_panics_doc,
        clippy::panic,
        clippy::unwrap_used
    )
)]

mod infographic;

use std::{
    array, env,
    fs::{self, File},
    io::{self, BufRead, BufReader, BufWriter, Read, Write},
    mem,
    path::{Path, PathBuf},
    str::FromStr,
    sync::Arc,
};

use anyhow::{Context, Result, bail};
use arrow_array::{ArrayRef, BooleanArray, Float64Array, RecordBatch, UInt16Array, UInt32Array};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use dotenvy::dotenv;
use flate2::read::MultiGzDecoder;
use indicatif::{ProgressBar, ProgressStyle};
use parquet::{arrow::ArrowWriter, basic::Compression, file::properties::WriterProperties};
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
pub use topology_classifier::{CHECK_COUNT, Check};
use topology_classifier::{
    Smiles, TopologyClassification, classify_smiles as classify_topology_smiles,
};
use zenodo_rs::{
    AccessRight, Creator, DepositMetadataUpdate, DepositionId, FileReplacePolicy, UploadSpec,
    UploadType, ZenodoClient,
};

/// Default PubChem snapshot URL used by the fixed-flow binary.
pub const DEFAULT_PUBCHEM_SMILES_URL: &str =
    "https://ftp.ncbi.nlm.nih.gov/pubchem/Compound/Extras/CID-SMILES.gz";
/// Default number of decompressed rows processed per Rayon batch.
pub const DEFAULT_BATCH_SIZE: usize = 32_768;
/// Default number of rows written per Parquet row group.
pub const DEFAULT_PARQUET_BATCH_ROWS: usize = 1_000_000;
/// Default creator name embedded in Zenodo metadata.
pub const DEFAULT_CREATOR_NAME: &str = "Luca Cappelletti";
/// Default ORCID embedded in Zenodo metadata.
pub const DEFAULT_CREATOR_ORCID: &str = "0000-0002-1269-2038";
const ZENODO_TOKEN_ENV: &str = "ZENODO_TOKEN";

const BYTES_PROGRESS_TEMPLATE: &str =
    "{msg:14} [{elapsed_precise}] {bar:40.cyan/blue} {bytes}/{total_bytes} ({eta})";
const ITEMS_PROGRESS_TEMPLATE: &str =
    "{msg:14} [{elapsed_precise}] {bar:40.cyan/blue} {pos}/{len} ({eta})";
const SPINNER_PROGRESS_TEMPLATE: &str = "{msg:14} [{elapsed_precise}] {spinner}";

const BOOL_COLUMN_COUNT: u64 = CHECK_COUNT as u64;
const U16_COLUMN_COUNT: u64 = 2;
const U32_COLUMN_COUNT: u64 = 2;
const F64_COLUMN_COUNT: u64 = 2;
const COEFFICIENT_HISTOGRAM_BUCKETS: usize = 10;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
/// One histogram bin for a component-count distribution.
pub struct ComponentHistogramBin {
    /// Number of components in the chosen graph decomposition.
    pub component_count: u16,
    /// Number of molecules with exactly this many components.
    pub molecules: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
/// One histogram bin for a scalar graph metric distribution.
pub struct ScalarHistogramBin {
    /// Observed scalar value.
    pub value: u32,
    /// Number of molecules with exactly this value.
    pub molecules: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
/// One histogram bin for a coefficient aggregated into a fixed numeric interval.
pub struct CoefficientHistogramBin {
    /// Inclusive lower bound of the coefficient bucket.
    pub lower_bound: f64,
    /// Inclusive upper bound of the coefficient bucket.
    pub upper_bound: f64,
    /// Number of molecules whose coefficient falls in this interval.
    pub molecules: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Controls how the input TSV is obtained before classification.
pub enum InputMode {
    /// Download the gzip snapshot and decompress it locally.
    DownloadAndDecompress,
    /// Skip download and use an already decompressed TSV file.
    UseExistingDecompressed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Controls whether the produced artifacts are published to Zenodo.
pub enum PublishMode {
    /// Publish to `zenodo.org`.
    Production,
    /// Publish to `sandbox.zenodo.org`.
    Sandbox,
    /// Skip publishing entirely.
    Skip,
}

impl PublishMode {
    fn label(self) -> &'static str {
        match self {
            Self::Production => "production",
            Self::Sandbox => "sandbox",
            Self::Skip => "skip",
        }
    }
}

#[derive(Debug, Clone)]
/// Fixed pipeline configuration for programmatic callers and tests.
pub struct PipelineConfig {
    /// Source URL for the PubChem gzip snapshot.
    pub pubchem_url: String,
    /// Local destination of the downloaded gzip snapshot.
    pub compressed_path: PathBuf,
    /// Local destination of the decompressed TSV snapshot.
    pub decompressed_path: PathBuf,
    /// Output Parquet file containing one row per CID.
    pub parquet_path: PathBuf,
    /// Output JSON summary file containing aggregate counts and run metadata.
    pub summary_path: PathBuf,
    /// Output SVG infographic summarizing the topology results.
    pub infographic_path: PathBuf,
    /// Local TOML file used to persist Zenodo deposition state.
    pub zenodo_state_path: PathBuf,
    /// Number of input rows processed together in one Rayon batch.
    pub batch_size: usize,
    /// Number of rows written together to one Parquet row group.
    pub parquet_batch_rows: usize,
    /// How the input TSV should be prepared.
    pub input_mode: InputMode,
    /// Whether and where the generated artifacts should be published.
    pub publish_mode: PublishMode,
}

impl Default for PipelineConfig {
    fn default() -> Self {
        Self {
            pubchem_url: DEFAULT_PUBCHEM_SMILES_URL.to_owned(),
            compressed_path: PathBuf::from("data/pubchem/CID-SMILES.gz"),
            decompressed_path: PathBuf::from("data/pubchem/CID-SMILES.tsv"),
            parquet_path: PathBuf::from("results/pubchem-topology.parquet"),
            summary_path: PathBuf::from("results/pubchem-topology-summary.json"),
            infographic_path: PathBuf::from("results/pubchem-topology-infographic.svg"),
            zenodo_state_path: PathBuf::from(".zenodo/production-state.toml"),
            batch_size: DEFAULT_BATCH_SIZE,
            parquet_batch_rows: DEFAULT_PARQUET_BATCH_ROWS,
            input_mode: InputMode::DownloadAndDecompress,
            publish_mode: PublishMode::Production,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
/// Aggregate report written alongside the Parquet dataset.
pub struct TopologyReport {
    /// Snapshot URL used for the input dataset.
    pub source_url: String,
    /// Local gzip snapshot path.
    pub compressed_path: PathBuf,
    /// Local decompressed TSV path.
    pub decompressed_path: PathBuf,
    /// Local Parquet output path.
    pub parquet_path: PathBuf,
    /// Local JSON summary output path.
    pub summary_path: PathBuf,
    /// Local SVG infographic output path.
    pub infographic_path: PathBuf,
    /// Size in bytes of the gzip snapshot, if present.
    pub compressed_bytes: u64,
    /// Size in bytes of the decompressed TSV snapshot.
    pub decompressed_bytes: u64,
    /// Number of input records seen.
    pub total_records: u64,
    /// Number of rows whose SMILES parsed successfully.
    pub parsed_records: u64,
    /// Number of rows rejected before topology evaluation.
    pub parse_errors: u64,
    /// Number of rows that parsed but failed a topology computation.
    pub topology_errors: u64,
    /// Wall-clock runtime of the analysis and artifact generation steps, in seconds.
    pub pipeline_elapsed_seconds: f64,
    /// Estimated memory footprint of the in-memory column store.
    pub estimated_result_memory_bytes: u64,
    /// Stable names matching the order of the count array.
    pub check_names: Vec<String>,
    /// Folded true counts for each topology predicate.
    pub counts: [u64; CHECK_COUNT],
    /// Histogram of the number of connected components per successfully evaluated molecule.
    pub connected_components_histogram: Vec<ComponentHistogramBin>,
    /// Histogram of exact undirected diameters for successfully evaluated connected molecules.
    pub diameter_histogram: Vec<ScalarHistogramBin>,
    /// Histogram of exact triangle counts for successfully evaluated molecules.
    pub triangle_count_histogram: Vec<ScalarHistogramBin>,
    /// Histogram of exact square counts for successfully evaluated molecules.
    pub square_count_histogram: Vec<ScalarHistogramBin>,
    /// Histogram of mean local clustering coefficients using fixed-width buckets.
    pub clustering_coefficient_histogram: Vec<CoefficientHistogramBin>,
    /// Histogram of mean square clustering coefficients using fixed-width buckets.
    pub square_clustering_coefficient_histogram: Vec<CoefficientHistogramBin>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
/// Summary of the published Zenodo record family after a successful upload.
pub struct ZenodoPublicationSummary {
    /// Target endpoint label used by the run.
    pub endpoint: String,
    /// Root deposition id reused across versions.
    pub root_deposition_id: u64,
    /// Most recent deposition id created or reused by the publish workflow.
    pub latest_deposition_id: u64,
    /// Most recent public record id, when available.
    pub record_id: Option<u64>,
    /// Concept record id shared by all versions, when available.
    pub concept_record_id: Option<u64>,
    /// Latest version DOI, when available.
    pub doi: Option<String>,
    /// Concept DOI shared across versions, when available.
    pub concept_doi: Option<String>,
}

#[derive(Debug, Clone)]
/// Full result of one fixed pipeline execution.
pub struct RunOutcome {
    /// Aggregate local report written to JSON.
    pub report: TopologyReport,
    /// Zenodo publication summary, if publishing was enabled.
    pub publication: Option<ZenodoPublicationSummary>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ZenodoState {
    endpoint: String,
    root_deposition_id: u64,
    latest_deposition_id: u64,
    latest_record_id: Option<u64>,
    concept_record_id: Option<u64>,
    doi: Option<String>,
    concept_doi: Option<String>,
}

#[derive(Debug, Clone)]
struct InputPreparation {
    compressed_bytes: u64,
    decompressed_bytes: u64,
}

#[derive(Debug, Clone, Default)]
struct BatchStats {
    total_records: u64,
    parsed_records: u64,
    parse_errors: u64,
    topology_errors: u64,
    counts: [u64; CHECK_COUNT],
    connected_components_histogram: Vec<u64>,
    diameter_histogram: Vec<u64>,
    triangle_count_histogram: Vec<u64>,
    square_count_histogram: Vec<u64>,
    clustering_coefficient_histogram: Vec<u64>,
    square_clustering_coefficient_histogram: Vec<u64>,
}

impl BatchStats {
    fn observe(&mut self, row: &RowClassification) {
        self.total_records += 1;
        match row.state {
            RowState::ParseError => {
                self.parse_errors += 1;
            }
            RowState::TopologyError => {
                self.parsed_records += 1;
                self.topology_errors += 1;
            }
            RowState::Success => {
                self.parsed_records += 1;
                for (slot, value) in self.counts.iter_mut().zip(row.checks) {
                    *slot += u64::from(value);
                }
                increment_component_histogram(
                    &mut self.connected_components_histogram,
                    row.connected_components,
                );
                if row.connected_components == 1 {
                    increment_scalar_histogram(
                        &mut self.diameter_histogram,
                        u32::from(row.diameter),
                    );
                }
                increment_scalar_histogram(&mut self.triangle_count_histogram, row.triangle_count);
                increment_scalar_histogram(&mut self.square_count_histogram, row.square_count);
                increment_coefficient_histogram(
                    &mut self.clustering_coefficient_histogram,
                    row.clustering_coefficient,
                );
                increment_coefficient_histogram(
                    &mut self.square_clustering_coefficient_histogram,
                    row.square_clustering_coefficient,
                );
            }
        }
    }

    fn merge(&mut self, other: Self) {
        self.total_records += other.total_records;
        self.parsed_records += other.parsed_records;
        self.parse_errors += other.parse_errors;
        self.topology_errors += other.topology_errors;
        for (slot, value) in self.counts.iter_mut().zip(other.counts) {
            *slot += value;
        }
        merge_histogram(
            &mut self.connected_components_histogram,
            other.connected_components_histogram,
        );
        merge_histogram(&mut self.diameter_histogram, other.diameter_histogram);
        merge_histogram(
            &mut self.triangle_count_histogram,
            other.triangle_count_histogram,
        );
        merge_histogram(
            &mut self.square_count_histogram,
            other.square_count_histogram,
        );
        merge_histogram(
            &mut self.clustering_coefficient_histogram,
            other.clustering_coefficient_histogram,
        );
        merge_histogram(
            &mut self.square_clustering_coefficient_histogram,
            other.square_clustering_coefficient_histogram,
        );
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RowState {
    ParseError,
    TopologyError,
    Success,
}

#[derive(Debug, Clone)]
struct RowClassification {
    cid: u32,
    state: RowState,
    checks: [bool; CHECK_COUNT],
    connected_components: u16,
    diameter: u16,
    triangle_count: u32,
    square_count: u32,
    clustering_coefficient: f64,
    square_clustering_coefficient: f64,
}

impl Default for RowClassification {
    fn default() -> Self {
        Self {
            cid: 0,
            state: RowState::ParseError,
            checks: [false; CHECK_COUNT],
            connected_components: 0,
            diameter: 0,
            triangle_count: 0,
            square_count: 0,
            clustering_coefficient: 0.0,
            square_clustering_coefficient: 0.0,
        }
    }
}

#[derive(Debug, Clone)]
struct BatchOutput {
    rows: Vec<RowClassification>,
    stats: BatchStats,
}

#[derive(Debug, Clone)]
struct TopologyColumns {
    cids: Vec<u32>,
    connected_components: Vec<u16>,
    diameter: Vec<u16>,
    triangle_count: Vec<u32>,
    square_count: Vec<u32>,
    clustering_coefficient: Vec<f64>,
    square_clustering_coefficient: Vec<f64>,
    checks: [Vec<bool>; CHECK_COUNT],
}

impl TopologyColumns {
    fn with_capacity(capacity: usize) -> Self {
        Self {
            cids: Vec::with_capacity(capacity),
            connected_components: Vec::with_capacity(capacity),
            diameter: Vec::with_capacity(capacity),
            triangle_count: Vec::with_capacity(capacity),
            square_count: Vec::with_capacity(capacity),
            clustering_coefficient: Vec::with_capacity(capacity),
            square_clustering_coefficient: Vec::with_capacity(capacity),
            checks: array::from_fn(|_| Vec::with_capacity(capacity)),
        }
    }

    fn len(&self) -> usize {
        self.cids.len()
    }

    fn extend(&mut self, rows: Vec<RowClassification>) {
        for row in rows {
            self.cids.push(row.cid);
            self.connected_components.push(row.connected_components);
            self.diameter.push(row.diameter);
            self.triangle_count.push(row.triangle_count);
            self.square_count.push(row.square_count);
            self.clustering_coefficient.push(row.clustering_coefficient);
            self.square_clustering_coefficient
                .push(row.square_clustering_coefficient);
            for (column, value) in self.checks.iter_mut().zip(row.checks) {
                column.push(value);
            }
        }
    }

    fn record_batch(&self, schema: SchemaRef, start: usize, end: usize) -> Result<RecordBatch> {
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(CHECK_COUNT + 7);
        arrays.push(Arc::new(UInt32Array::from_iter_values(
            self.cids[start..end].iter().copied(),
        )));
        arrays.push(Arc::new(UInt16Array::from_iter_values(
            self.connected_components[start..end].iter().copied(),
        )));
        arrays.push(Arc::new(UInt16Array::from_iter(
            self.diameter[start..end]
                .iter()
                .copied()
                .zip(self.connected_components[start..end].iter().copied())
                .map(|(diameter, connected_components)| {
                    (connected_components == 1).then_some(diameter)
                }),
        )));
        arrays.push(Arc::new(UInt32Array::from_iter_values(
            self.triangle_count[start..end].iter().copied(),
        )));
        arrays.push(Arc::new(UInt32Array::from_iter_values(
            self.square_count[start..end].iter().copied(),
        )));
        arrays.push(Arc::new(Float64Array::from_iter_values(
            self.clustering_coefficient[start..end].iter().copied(),
        )));
        arrays.push(Arc::new(Float64Array::from_iter_values(
            self.square_clustering_coefficient[start..end]
                .iter()
                .copied(),
        )));

        for column in &self.checks {
            arrays.push(Arc::new(BooleanArray::from_iter(
                column[start..end].iter().copied(),
            )));
        }

        RecordBatch::try_new(schema, arrays).context("failed to build Parquet record batch")
    }
}

/// Runs the default fixed-flow pipeline used by the binary.
///
/// # Errors
///
/// Returns an error if input preparation, classification, Parquet writing,
/// summary writing, or publishing fails.
pub fn run() -> Result<RunOutcome> {
    run_with_config(&PipelineConfig::default())
}

/// Runs the topology pipeline with an explicit configuration.
///
/// # Errors
///
/// Returns an error if configuration validation, input preparation,
/// classification, Parquet writing, summary writing, or publishing fails.
pub fn run_with_config(config: &PipelineConfig) -> Result<RunOutcome> {
    let started_at = std::time::Instant::now();
    dotenv().ok();
    validate_config(config)?;

    let production_token_present = env_var_is_configured(ZENODO_TOKEN_ENV);
    if let Some(warning) =
        missing_publication_warning(config.publish_mode, production_token_present)
    {
        eprintln!("{warning}");
    }

    let zenodo_client = build_zenodo_client(config.publish_mode, production_token_present)?;

    ensure_parent_dir(&config.compressed_path)?;
    ensure_parent_dir(&config.decompressed_path)?;
    ensure_parent_dir(&config.parquet_path)?;
    ensure_parent_dir(&config.summary_path)?;
    ensure_parent_dir(&config.infographic_path)?;
    ensure_parent_dir(&config.zenodo_state_path)?;

    let input = prepare_input(config)?;
    let total_records = count_pubchem_records(&config.decompressed_path)?;
    let estimated_memory = estimate_result_memory_bytes(total_records);

    println!(
        "allocating in-memory columns for {total_records} records (~{})",
        format_gibibytes(estimated_memory)
    );

    let (columns, stats) =
        classify_pubchem_smiles(&config.decompressed_path, config.batch_size, total_records)?;
    if stats.total_records != total_records {
        bail!(
            "counted {total_records} records but classified {} records",
            stats.total_records
        );
    }

    write_parquet(&columns, &config.parquet_path, config.parquet_batch_rows)?;

    let mut report = TopologyReport {
        source_url: config.pubchem_url.clone(),
        compressed_path: config.compressed_path.clone(),
        decompressed_path: config.decompressed_path.clone(),
        parquet_path: config.parquet_path.clone(),
        summary_path: config.summary_path.clone(),
        infographic_path: config.infographic_path.clone(),
        compressed_bytes: input.compressed_bytes,
        decompressed_bytes: input.decompressed_bytes,
        total_records: stats.total_records,
        parsed_records: stats.parsed_records,
        parse_errors: stats.parse_errors,
        topology_errors: stats.topology_errors,
        pipeline_elapsed_seconds: 0.0,
        estimated_result_memory_bytes: estimated_memory,
        check_names: Check::ALL
            .into_iter()
            .map(Check::name)
            .map(str::to_owned)
            .collect(),
        counts: stats.counts,
        connected_components_histogram: histogram_bins(&stats.connected_components_histogram),
        diameter_histogram: scalar_histogram_bins(&stats.diameter_histogram),
        triangle_count_histogram: scalar_histogram_bins(&stats.triangle_count_histogram),
        square_count_histogram: scalar_histogram_bins(&stats.square_count_histogram),
        clustering_coefficient_histogram: coefficient_histogram_bins(
            &stats.clustering_coefficient_histogram,
        ),
        square_clustering_coefficient_histogram: coefficient_histogram_bins(
            &stats.square_clustering_coefficient_histogram,
        ),
    };

    report.pipeline_elapsed_seconds = started_at.elapsed().as_secs_f64();
    infographic::write_infographic(&report, &config.infographic_path)?;
    report.pipeline_elapsed_seconds = started_at.elapsed().as_secs_f64();
    write_summary(&report, &config.summary_path)?;

    let publication = if let Some(client) = zenodo_client {
        let runtime = tokio::runtime::Runtime::new().context("failed to create Tokio runtime")?;
        Some(runtime.block_on(publish_artifacts(config, &report, &client))?)
    } else {
        None
    };

    Ok(RunOutcome {
        report,
        publication,
    })
}

fn validate_config(config: &PipelineConfig) -> Result<()> {
    if config.batch_size == 0 {
        bail!("batch size must be greater than zero");
    }
    if config.parquet_batch_rows == 0 {
        bail!("Parquet batch rows must be greater than zero");
    }
    Ok(())
}

fn build_zenodo_client(
    mode: PublishMode,
    production_token_present: bool,
) -> Result<Option<ZenodoClient>> {
    match mode {
        PublishMode::Production if !production_token_present => Ok(None),
        PublishMode::Production => Ok(Some(ZenodoClient::from_env()?)),
        PublishMode::Sandbox => Ok(Some(ZenodoClient::from_sandbox_env()?)),
        PublishMode::Skip => Ok(None),
    }
}

fn env_var_is_configured(name: &str) -> bool {
    env::var_os(name).is_some_and(|value| !value.to_string_lossy().trim().is_empty())
}

fn missing_publication_warning(
    mode: PublishMode,
    production_token_present: bool,
) -> Option<&'static str> {
    match mode {
        PublishMode::Production if !production_token_present => {
            Some("warning: ZENODO_TOKEN is not set; continuing without Zenodo publication")
        }
        _ => None,
    }
}

fn prepare_input(config: &PipelineConfig) -> Result<InputPreparation> {
    match config.input_mode {
        InputMode::DownloadAndDecompress => {
            if config.decompressed_path.exists() {
                println!(
                    "reusing existing decompressed input {}",
                    config.decompressed_path.display()
                );

                let compressed_bytes = if config.compressed_path.exists() {
                    file_len(&config.compressed_path)?
                } else {
                    0
                };

                return Ok(InputPreparation {
                    compressed_bytes,
                    decompressed_bytes: file_len(&config.decompressed_path)?,
                });
            }

            if config.compressed_path.exists() {
                println!(
                    "reusing existing compressed input {}",
                    config.compressed_path.display()
                );
                decompress_gzip(&config.compressed_path, &config.decompressed_path)?;

                return Ok(InputPreparation {
                    compressed_bytes: file_len(&config.compressed_path)?,
                    decompressed_bytes: file_len(&config.decompressed_path)?,
                });
            }

            download_file(&config.pubchem_url, &config.compressed_path)?;
            decompress_gzip(&config.compressed_path, &config.decompressed_path)?;

            Ok(InputPreparation {
                compressed_bytes: file_len(&config.compressed_path)?,
                decompressed_bytes: file_len(&config.decompressed_path)?,
            })
        }
        InputMode::UseExistingDecompressed => {
            if !config.decompressed_path.exists() {
                bail!(
                    "expected existing decompressed input at {}",
                    config.decompressed_path.display()
                );
            }

            let compressed_bytes = if config.compressed_path.exists() {
                file_len(&config.compressed_path)?
            } else {
                0
            };

            Ok(InputPreparation {
                compressed_bytes,
                decompressed_bytes: file_len(&config.decompressed_path)?,
            })
        }
    }
}

fn ensure_parent_dir(path: &Path) -> Result<()> {
    let Some(parent) = path.parent() else {
        bail!("path {} does not have a parent directory", path.display());
    };
    fs::create_dir_all(parent)
        .with_context(|| format!("failed to create directory {}", parent.display()))?;
    Ok(())
}

fn file_len(path: &Path) -> Result<u64> {
    fs::metadata(path)
        .with_context(|| format!("failed to stat {}", path.display()))
        .map(|metadata| metadata.len())
}

fn download_file(url: &str, destination: &Path) -> Result<()> {
    println!("downloading {url} -> {}", destination.display());

    let client = reqwest::blocking::Client::builder()
        .build()
        .context("failed to build HTTP client")?;
    let response = client
        .get(url)
        .send()
        .with_context(|| format!("failed to GET {url}"))?
        .error_for_status()
        .with_context(|| format!("unsuccessful response while downloading {url}"))?;

    let total_bytes = response.content_length().unwrap_or(0);
    let progress_bar = new_bytes_progress_bar("download", total_bytes);
    let mut reader = ProgressReader::new(response, progress_bar.clone());

    let temp_path = temporary_path(destination, "download");
    let mut writer = BufWriter::new(
        File::create(&temp_path)
            .with_context(|| format!("failed to create {}", temp_path.display()))?,
    );

    io::copy(&mut reader, &mut writer)
        .with_context(|| format!("failed to stream download into {}", temp_path.display()))?;
    writer
        .flush()
        .with_context(|| format!("failed to flush {}", temp_path.display()))?;

    progress_bar.finish_with_message(format!("download       {}", destination.display()));
    fs::rename(&temp_path, destination).with_context(|| {
        format!(
            "failed to move completed download from {} to {}",
            temp_path.display(),
            destination.display()
        )
    })?;
    Ok(())
}

fn decompress_gzip(source: &Path, destination: &Path) -> Result<()> {
    println!(
        "decompressing {} -> {}",
        source.display(),
        destination.display()
    );

    let compressed_bytes = file_len(source)?;
    let progress_bar = new_bytes_progress_bar("decompress", compressed_bytes);

    let file =
        File::open(source).with_context(|| format!("failed to open {}", source.display()))?;
    let reader = ProgressReader::new(file, progress_bar.clone());
    let mut decoder = MultiGzDecoder::new(reader);

    let temp_path = temporary_path(destination, "decompress");
    let mut writer = BufWriter::new(
        File::create(&temp_path)
            .with_context(|| format!("failed to create {}", temp_path.display()))?,
    );

    io::copy(&mut decoder, &mut writer).with_context(|| {
        format!(
            "failed to decompress {} into {}",
            source.display(),
            temp_path.display()
        )
    })?;
    writer
        .flush()
        .with_context(|| format!("failed to flush {}", temp_path.display()))?;

    progress_bar.finish_with_message(format!("decompress     {}", destination.display()));
    fs::rename(&temp_path, destination).with_context(|| {
        format!(
            "failed to move decompressed file from {} to {}",
            temp_path.display(),
            destination.display()
        )
    })?;
    Ok(())
}

fn count_pubchem_records(path: &Path) -> Result<u64> {
    println!("counting rows in {}", path.display());

    let total_bytes = file_len(path)?;
    let progress_bar = new_bytes_progress_bar("count rows", total_bytes);
    let file = File::open(path).with_context(|| format!("failed to open {}", path.display()))?;
    let mut reader = ProgressReader::new(BufReader::new(file), progress_bar.clone());
    let mut buffer = [0_u8; 1024 * 1024];
    let mut rows = 0_u64;
    let mut last_byte = None;

    loop {
        let bytes_read = reader
            .read(&mut buffer)
            .with_context(|| format!("failed reading from {}", path.display()))?;
        if bytes_read == 0 {
            break;
        }

        for byte in &buffer[..bytes_read] {
            if *byte == b'\n' {
                rows += 1;
            }
        }
        last_byte = Some(buffer[bytes_read - 1]);
    }

    if let Some(byte) = last_byte
        && byte != b'\n'
    {
        rows += 1;
    }

    progress_bar.finish_with_message(format!("count rows     {}", path.display()));
    Ok(rows)
}

fn classify_pubchem_smiles(
    path: &Path,
    batch_size: usize,
    total_records: u64,
) -> Result<(TopologyColumns, BatchStats)> {
    println!("classifying {}", path.display());

    let capacity =
        usize::try_from(total_records).context("record count does not fit into usize")?;
    let total_bytes = file_len(path)?;
    let progress_bar = new_bytes_progress_bar("classify", total_bytes);

    let file = File::open(path).with_context(|| format!("failed to open {}", path.display()))?;
    let mut reader = BufReader::new(file);
    let mut line = String::new();
    let mut batch = Vec::with_capacity(batch_size);
    let mut batch_bytes = 0_u64;
    let mut columns = TopologyColumns::with_capacity(capacity);
    let mut totals = BatchStats::default();

    loop {
        line.clear();
        let bytes_read = reader
            .read_line(&mut line)
            .with_context(|| format!("failed reading from {}", path.display()))?;
        if bytes_read == 0 {
            break;
        }

        batch_bytes += bytes_read as u64;
        batch.push(mem::take(&mut line));

        if batch.len() == batch_size {
            let output = process_batch(mem::take(&mut batch));
            totals.merge(output.stats);
            columns.extend(output.rows);
            progress_bar.inc(batch_bytes);
            batch_bytes = 0;
        }
    }

    if !batch.is_empty() {
        let output = process_batch(batch);
        totals.merge(output.stats);
        columns.extend(output.rows);
        progress_bar.inc(batch_bytes);
    }

    progress_bar.finish_with_message(format!("classify       {}", path.display()));
    Ok((columns, totals))
}

fn process_batch(batch: Vec<String>) -> BatchOutput {
    let rows: Vec<RowClassification> = batch
        .into_par_iter()
        .map(|record| classify_record(&record))
        .collect();

    let mut stats = BatchStats::default();
    for row in &rows {
        stats.observe(row);
    }

    BatchOutput { rows, stats }
}

fn classify_record(record: &str) -> RowClassification {
    classify_record_with(record, classify_topology_smiles)
}

fn classify_record_with<F, E>(record: &str, classify: F) -> RowClassification
where
    F: Fn(&Smiles) -> std::result::Result<TopologyClassification, E>,
{
    let Some((cid_text, smiles_text)) = parse_pubchem_record(record) else {
        return RowClassification::default();
    };

    let cid = match cid_text.parse::<u32>() {
        Ok(cid) => cid,
        Err(_) => return RowClassification::default(),
    };

    let smiles = match Smiles::from_str(smiles_text) {
        Ok(smiles) => smiles,
        Err(_) => {
            return RowClassification {
                cid,
                ..RowClassification::default()
            };
        }
    };

    match classify(&smiles) {
        Ok(classification) => RowClassification {
            cid,
            state: RowState::Success,
            checks: classification.checks,
            connected_components: classification.connected_components,
            diameter: classification.diameter.unwrap_or(0),
            triangle_count: classification.triangle_count,
            square_count: classification.square_count,
            clustering_coefficient: classification.clustering_coefficient,
            square_clustering_coefficient: classification.square_clustering_coefficient,
        },
        Err(_) => RowClassification {
            cid,
            state: RowState::TopologyError,
            checks: [false; CHECK_COUNT],
            connected_components: 0,
            diameter: 0,
            triangle_count: 0,
            square_count: 0,
            clustering_coefficient: 0.0,
            square_clustering_coefficient: 0.0,
        },
    }
}

fn parse_pubchem_record(record: &str) -> Option<(&str, &str)> {
    let trimmed = record.trim_end_matches(['\n', '\r']);
    if trimmed.is_empty() {
        return None;
    }

    if let Some((cid, smiles)) = trimmed.split_once('\t')
        && !cid.is_empty()
        && !smiles.is_empty()
    {
        return Some((cid, smiles));
    }

    let mut fields = trimmed.split_whitespace();
    let cid = fields.next()?;
    let smiles = fields.next()?;
    Some((cid, smiles))
}

fn write_parquet(columns: &TopologyColumns, path: &Path, batch_rows: usize) -> Result<()> {
    println!("writing Parquet {}", path.display());

    let temp_path = temporary_path(path, "parquet");
    let schema = parquet_schema();
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(Default::default()))
        .set_max_row_group_row_count(Some(batch_rows))
        .build();
    let total_rows = columns.len();
    let progress_bar = new_items_progress_bar("write parquet", total_rows as u64);

    let writer = BufWriter::new(
        File::create(&temp_path)
            .with_context(|| format!("failed to create {}", temp_path.display()))?,
    );
    let mut parquet_writer = ArrowWriter::try_new(writer, schema.clone(), Some(props))
        .context("failed to open Parquet writer")?;

    let mut start = 0_usize;
    while start < total_rows {
        let end = (start + batch_rows).min(total_rows);
        let batch = columns.record_batch(schema.clone(), start, end)?;
        parquet_writer
            .write(&batch)
            .with_context(|| format!("failed to write Parquet rows {start}..{end}"))?;
        progress_bar.inc((end - start) as u64);
        start = end;
    }

    parquet_writer
        .close()
        .context("failed to finalize Parquet writer")?;
    progress_bar.finish_with_message(format!("write parquet  {}", path.display()));
    fs::rename(&temp_path, path).with_context(|| {
        format!(
            "failed to move Parquet file from {} to {}",
            temp_path.display(),
            path.display()
        )
    })?;
    Ok(())
}

fn parquet_schema() -> SchemaRef {
    let mut fields = vec![
        Field::new("cid", DataType::UInt32, false),
        Field::new("connected_components", DataType::UInt16, false),
        Field::new("diameter", DataType::UInt16, true),
        Field::new("triangle_count", DataType::UInt32, false),
        Field::new("square_count", DataType::UInt32, false),
        Field::new("clustering_coefficient", DataType::Float64, false),
        Field::new("square_clustering_coefficient", DataType::Float64, false),
    ];
    for check in Check::ALL {
        fields.push(Field::new(check.name(), DataType::Boolean, false));
    }
    Arc::new(Schema::new(fields))
}

fn write_summary(report: &TopologyReport, path: &Path) -> Result<()> {
    let progress_bar = new_spinner("write summary");
    let temp_path = temporary_path(path, "summary");
    let mut writer = BufWriter::new(
        File::create(&temp_path)
            .with_context(|| format!("failed to create {}", temp_path.display()))?,
    );
    serde_json::to_writer_pretty(&mut writer, report)
        .with_context(|| format!("failed to serialize {}", temp_path.display()))?;
    writer
        .write_all(b"\n")
        .with_context(|| format!("failed to finalize {}", temp_path.display()))?;
    writer
        .flush()
        .with_context(|| format!("failed to flush {}", temp_path.display()))?;
    fs::rename(&temp_path, path).with_context(|| {
        format!(
            "failed to move summary from {} to {}",
            temp_path.display(),
            path.display()
        )
    })?;
    progress_bar.finish_with_message(format!("write summary  {}", path.display()));
    Ok(())
}

async fn publish_artifacts(
    config: &PipelineConfig,
    report: &TopologyReport,
    client: &ZenodoClient,
) -> Result<ZenodoPublicationSummary> {
    let progress_bar = new_spinner("publish");

    progress_bar.set_message("publish state");
    let mut state = if let Some(existing) = load_zenodo_state(&config.zenodo_state_path)? {
        if existing.endpoint != config.publish_mode.label() {
            bail!(
                "Zenodo state file {} targets {} but this run uses {}",
                config.zenodo_state_path.display(),
                existing.endpoint,
                config.publish_mode.label()
            );
        }
        existing
    } else {
        progress_bar.set_message("publish create");
        let deposition = client
            .create_deposition()
            .await
            .context("failed to create first Zenodo deposition")?;
        let state = ZenodoState {
            endpoint: config.publish_mode.label().to_owned(),
            root_deposition_id: deposition.id.0,
            latest_deposition_id: deposition.id.0,
            latest_record_id: deposition.record_id.map(|value| value.0),
            concept_record_id: deposition.conceptrecid.map(|value| value.0),
            doi: deposition.doi.map(|value| value.to_string()),
            concept_doi: deposition.conceptdoi.map(|value| value.to_string()),
        };
        write_zenodo_state(&config.zenodo_state_path, &state)?;
        state
    };

    progress_bar.set_message("publish files");
    let metadata = build_zenodo_metadata(report)?;
    let files = vec![
        UploadSpec::from_path(&config.parquet_path)
            .with_context(|| format!("failed to prepare {}", config.parquet_path.display()))?,
        UploadSpec::from_path(&config.summary_path)
            .with_context(|| format!("failed to prepare {}", config.summary_path.display()))?,
        UploadSpec::from_path(&config.infographic_path)
            .with_context(|| format!("failed to prepare {}", config.infographic_path.display()))?,
    ];

    let published = client
        .publish_dataset_with_policy(
            DepositionId(state.root_deposition_id),
            &metadata,
            FileReplacePolicy::ReplaceAll,
            files,
        )
        .await
        .context("failed to publish artifacts to Zenodo")?;

    state.latest_deposition_id = published.deposition.id.0;
    state.latest_record_id = Some(published.record.id.0);
    state.concept_record_id = published.record.conceptrecid.map(|value| value.0);
    state.doi = published.record.doi.map(|value| value.to_string());
    state.concept_doi = published.record.conceptdoi.map(|value| value.to_string());
    write_zenodo_state(&config.zenodo_state_path, &state)?;

    progress_bar.finish_with_message(format!(
        "publish        {}",
        config.zenodo_state_path.display()
    ));

    Ok(ZenodoPublicationSummary {
        endpoint: state.endpoint.clone(),
        root_deposition_id: state.root_deposition_id,
        latest_deposition_id: state.latest_deposition_id,
        record_id: state.latest_record_id,
        concept_record_id: state.concept_record_id,
        doi: state.doi.clone(),
        concept_doi: state.concept_doi.clone(),
    })
}

fn build_zenodo_metadata(report: &TopologyReport) -> Result<DepositMetadataUpdate> {
    let creator = Creator::builder()
        .name(DEFAULT_CREATOR_NAME)
        .orcid(DEFAULT_CREATOR_ORCID)
        .build()
        .context("failed to build Zenodo creator metadata")?;

    DepositMetadataUpdate::builder()
        .title(zenodo_title())
        .upload_type(UploadType::Dataset)
        .description_html(zenodo_description_html(report))
        .creator(creator)
        .access_right(AccessRight::Open)
        .keyword("PubChem")
        .keyword("SMILES")
        .keyword("cheminformatics")
        .keyword("graph topology")
        .keyword("parquet")
        .notes(zenodo_notes(report))
        .build()
        .context("failed to build Zenodo metadata")
}

fn zenodo_title() -> &'static str {
    "PubChem CID-SMILES topology classification snapshot"
}

fn zenodo_description_html(report: &TopologyReport) -> String {
    let checks = Check::ALL
        .into_iter()
        .map(Check::name)
        .collect::<Vec<_>>()
        .join(", ");

    format!(
        "<p>Topology annotations for the current PubChem CID-SMILES snapshot.</p>\
         <p>The Parquet artifact stores one row per PubChem CID with connected-component counts, exact diameters for connected molecules, triangle and square motif counts, mean local and square clustering coefficients, and the following topology predicates computed with <code>smiles-parser</code> and <code>geometric-traits</code>: {checks}.</p>\
         <p>The JSON sidecar stores aggregate counts, parse and topology error totals, and run metadata, while the SVG infographic provides an accessible visual summary of the run. Source snapshot URL: <code>{}</code>.</p>",
        report.source_url,
    )
}

fn zenodo_notes(report: &TopologyReport) -> String {
    format!(
        "Rows: {}. Parsed: {}. Parse errors: {}. Topology errors: {}. Estimated in-memory result size: {}. Analysis runtime: {} seconds.",
        report.total_records,
        report.parsed_records,
        report.parse_errors,
        report.topology_errors,
        format_gibibytes(report.estimated_result_memory_bytes),
        report.pipeline_elapsed_seconds.round() as u64,
    )
}

fn load_zenodo_state(path: &Path) -> Result<Option<ZenodoState>> {
    if !path.exists() {
        return Ok(None);
    }

    let state =
        fs::read_to_string(path).with_context(|| format!("failed to read {}", path.display()))?;
    let data =
        toml::from_str(&state).with_context(|| format!("failed to parse {}", path.display()))?;
    Ok(Some(data))
}

fn write_zenodo_state(path: &Path, state: &ZenodoState) -> Result<()> {
    let temp_path = temporary_path(path, "zenodo-state");
    let mut writer = BufWriter::new(
        File::create(&temp_path)
            .with_context(|| format!("failed to create {}", temp_path.display()))?,
    );

    writer.write_all(
        b"# Persistent Zenodo deposition state for pubchem-topology.\n\
          # The root deposition id is reused on future runs to create new versions.\n\
          # Production and sandbox runs should always use separate state files.\n\n",
    )?;
    let body = toml::to_string_pretty(state).context("failed to serialize Zenodo state as TOML")?;
    writer
        .write_all(body.as_bytes())
        .with_context(|| format!("failed to write {}", temp_path.display()))?;
    writer
        .flush()
        .with_context(|| format!("failed to flush {}", temp_path.display()))?;
    fs::rename(&temp_path, path).with_context(|| {
        format!(
            "failed to move Zenodo state from {} to {}",
            temp_path.display(),
            path.display()
        )
    })?;
    Ok(())
}

fn temporary_path(path: &Path, suffix: &str) -> PathBuf {
    let mut file_name = path
        .file_name()
        .unwrap_or_default()
        .to_string_lossy()
        .into_owned();
    file_name.push('.');
    file_name.push_str(suffix);
    file_name.push_str(".tmp");
    path.with_file_name(file_name)
}

fn estimate_result_memory_bytes(total_records: u64) -> u64 {
    let cid_bytes = total_records.saturating_mul(mem::size_of::<u32>() as u64);
    let u16_bytes =
        U16_COLUMN_COUNT.saturating_mul(total_records.saturating_mul(mem::size_of::<u16>() as u64));
    let u32_bytes =
        U32_COLUMN_COUNT.saturating_mul(total_records.saturating_mul(mem::size_of::<u32>() as u64));
    let f64_bytes =
        F64_COLUMN_COUNT.saturating_mul(total_records.saturating_mul(mem::size_of::<f64>() as u64));
    let bool_bytes = BOOL_COLUMN_COUNT.saturating_mul(total_records.div_ceil(8));
    cid_bytes
        .saturating_add(u16_bytes)
        .saturating_add(u32_bytes)
        .saturating_add(f64_bytes)
        .saturating_add(bool_bytes)
}

fn increment_component_histogram(histogram: &mut Vec<u64>, component_count: u16) {
    let index = usize::from(component_count);
    if histogram.len() <= index {
        histogram.resize(index + 1, 0);
    }
    histogram[index] += 1;
}

fn increment_scalar_histogram(histogram: &mut Vec<u64>, value: u32) {
    let index = value as usize;
    if histogram.len() <= index {
        histogram.resize(index + 1, 0);
    }
    histogram[index] += 1;
}

fn increment_coefficient_histogram(histogram: &mut Vec<u64>, value: f64) {
    if histogram.len() < COEFFICIENT_HISTOGRAM_BUCKETS {
        histogram.resize(COEFFICIENT_HISTOGRAM_BUCKETS, 0);
    }
    let clamped = value.clamp(0.0, 1.0);
    let index = ((clamped * COEFFICIENT_HISTOGRAM_BUCKETS as f64).floor() as usize)
        .min(COEFFICIENT_HISTOGRAM_BUCKETS - 1);
    histogram[index] += 1;
}

fn merge_histogram(target: &mut Vec<u64>, source: Vec<u64>) {
    if source.len() > target.len() {
        target.resize(source.len(), 0);
    }
    for (slot, value) in target.iter_mut().zip(source) {
        *slot += value;
    }
}

fn histogram_bins(histogram: &[u64]) -> Vec<ComponentHistogramBin> {
    histogram
        .iter()
        .enumerate()
        .filter(|(component_count, molecules)| *component_count > 0 && **molecules > 0)
        .map(|(component_count, &molecules)| ComponentHistogramBin {
            component_count: component_count as u16,
            molecules,
        })
        .collect()
}

fn scalar_histogram_bins(histogram: &[u64]) -> Vec<ScalarHistogramBin> {
    histogram
        .iter()
        .enumerate()
        .filter(|(_, molecules)| **molecules > 0)
        .map(|(value, &molecules)| ScalarHistogramBin {
            value: value as u32,
            molecules,
        })
        .collect()
}

fn coefficient_histogram_bins(histogram: &[u64]) -> Vec<CoefficientHistogramBin> {
    (0..COEFFICIENT_HISTOGRAM_BUCKETS)
        .map(|index| CoefficientHistogramBin {
            lower_bound: index as f64 / COEFFICIENT_HISTOGRAM_BUCKETS as f64,
            upper_bound: (index + 1) as f64 / COEFFICIENT_HISTOGRAM_BUCKETS as f64,
            molecules: histogram.get(index).copied().unwrap_or(0),
        })
        .collect()
}

fn format_gibibytes(bytes: u64) -> String {
    format!("{:.2} GiB", bytes as f64 / 1024_f64 / 1024_f64 / 1024_f64)
}

fn new_bytes_progress_bar(phase: &'static str, total_bytes: u64) -> ProgressBar {
    if total_bytes == 0 {
        return new_spinner(phase);
    }

    let progress_bar = ProgressBar::new(total_bytes);
    let style = match ProgressStyle::with_template(BYTES_PROGRESS_TEMPLATE) {
        Ok(style) => style.progress_chars("=> "),
        Err(_) => ProgressStyle::default_bar(),
    };
    progress_bar.set_style(style);
    progress_bar.set_message(phase);
    progress_bar
}

fn new_items_progress_bar(phase: &'static str, total_items: u64) -> ProgressBar {
    let progress_bar = ProgressBar::new(total_items);
    let style = match ProgressStyle::with_template(ITEMS_PROGRESS_TEMPLATE) {
        Ok(style) => style.progress_chars("=> "),
        Err(_) => ProgressStyle::default_bar(),
    };
    progress_bar.set_style(style);
    progress_bar.set_message(phase);
    progress_bar
}

fn new_spinner(phase: &'static str) -> ProgressBar {
    let progress_bar = ProgressBar::new_spinner();
    let style = match ProgressStyle::with_template(SPINNER_PROGRESS_TEMPLATE) {
        Ok(style) => style,
        Err(_) => ProgressStyle::default_spinner(),
    };
    progress_bar.set_style(style);
    progress_bar.enable_steady_tick(std::time::Duration::from_millis(100));
    progress_bar.set_message(phase);
    progress_bar
}

struct ProgressReader<R> {
    inner: R,
    progress_bar: ProgressBar,
}

impl<R> ProgressReader<R> {
    fn new(inner: R, progress_bar: ProgressBar) -> Self {
        Self {
            inner,
            progress_bar,
        }
    }
}

impl<R: Read> Read for ProgressReader<R> {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        let bytes_read = self.inner.read(buffer)?;
        if bytes_read > 0 {
            self.progress_bar.inc(bytes_read as u64);
        }
        Ok(bytes_read)
    }
}

#[cfg(test)]
mod tests {
    use super::{
        BatchStats, CHECK_COUNT, Check, CoefficientHistogramBin, ComponentHistogramBin,
        DEFAULT_BATCH_SIZE, DEFAULT_PARQUET_BATCH_ROWS, DEFAULT_PUBCHEM_SMILES_URL, InputMode,
        PipelineConfig, PublishMode, RowClassification, RowState, ScalarHistogramBin, Smiles,
        TopologyColumns, TopologyReport, ZenodoState, build_zenodo_client, build_zenodo_metadata,
        classify_pubchem_smiles, classify_record, classify_record_with, classify_topology_smiles,
        count_pubchem_records, ensure_parent_dir, estimate_result_memory_bytes, format_gibibytes,
        load_zenodo_state, missing_publication_warning, new_bytes_progress_bar,
        new_items_progress_bar, new_spinner, parquet_schema, parse_pubchem_record, prepare_input,
        publish_artifacts, run_with_config, temporary_path, validate_config, write_parquet,
        write_summary, write_zenodo_state,
    };
    use std::{
        fs,
        fs::File,
        io::{Read, Write},
        net::TcpListener,
        path::{Path, PathBuf},
        thread,
    };

    use anyhow::{Result, anyhow};
    use flate2::{Compression as GzCompression, write::GzEncoder};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use tempfile::tempdir;
    use zenodo_rs::{Auth, ZenodoClient};

    const SAMPLE_TSV: &str = "2244\tCCO\n240\tC1CCCCC1\n241\tC1=CC=CC=C1\n999\tINVALID\n";

    fn write_gzip(contents: &str) -> Result<Vec<u8>> {
        let mut encoder = GzEncoder::new(Vec::new(), GzCompression::default());
        encoder.write_all(contents.as_bytes())?;
        Ok(encoder.finish()?)
    }

    fn write_text(path: &Path, contents: &str) -> Result<()> {
        fs::write(path, contents)?;
        Ok(())
    }

    fn spawn_http_server(body: Vec<u8>) -> Result<(String, thread::JoinHandle<Result<()>>)> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let address = listener.local_addr()?;
        let handle = thread::spawn(move || -> Result<()> {
            let (mut stream, _) = listener.accept()?;
            let mut request = [0_u8; 4096];
            let _ = stream.read(&mut request)?;
            write!(
                stream,
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            )?;
            stream.write_all(&body)?;
            Ok(())
        });

        Ok((format!("http://{address}/CID-SMILES.gz"), handle))
    }

    fn sample_report() -> TopologyReport {
        TopologyReport {
            source_url: DEFAULT_PUBCHEM_SMILES_URL.to_owned(),
            compressed_path: PathBuf::from("data/pubchem/CID-SMILES.gz"),
            decompressed_path: PathBuf::from("data/pubchem/CID-SMILES.tsv"),
            parquet_path: PathBuf::from("results/pubchem-topology.parquet"),
            summary_path: PathBuf::from("results/pubchem-topology-summary.json"),
            infographic_path: PathBuf::from("results/pubchem-topology-infographic.svg"),
            compressed_bytes: 10,
            decompressed_bytes: 20,
            total_records: 4,
            parsed_records: 3,
            parse_errors: 1,
            topology_errors: 0,
            pipeline_elapsed_seconds: 97.0,
            estimated_result_memory_bytes: 64,
            check_names: Check::ALL
                .into_iter()
                .map(Check::name)
                .map(str::to_owned)
                .collect(),
            counts: [1, 1, 3, 1, 3, 3, 0, 0, 0, 3],
            connected_components_histogram: vec![ComponentHistogramBin {
                component_count: 1,
                molecules: 3,
            }],
            diameter_histogram: vec![
                ScalarHistogramBin {
                    value: 2,
                    molecules: 1,
                },
                ScalarHistogramBin {
                    value: 3,
                    molecules: 2,
                },
            ],
            triangle_count_histogram: vec![
                ScalarHistogramBin {
                    value: 0,
                    molecules: 2,
                },
                ScalarHistogramBin {
                    value: 1,
                    molecules: 1,
                },
            ],
            square_count_histogram: vec![ScalarHistogramBin {
                value: 0,
                molecules: 3,
            }],
            clustering_coefficient_histogram: vec![
                CoefficientHistogramBin {
                    lower_bound: 0.0,
                    upper_bound: 0.1,
                    molecules: 2,
                },
                CoefficientHistogramBin {
                    lower_bound: 0.9,
                    upper_bound: 1.0,
                    molecules: 1,
                },
            ],
            square_clustering_coefficient_histogram: vec![CoefficientHistogramBin {
                lower_bound: 0.0,
                upper_bound: 0.1,
                molecules: 3,
            }],
        }
    }

    fn sample_state(endpoint: &str) -> ZenodoState {
        ZenodoState {
            endpoint: endpoint.to_owned(),
            root_deposition_id: 1,
            latest_deposition_id: 2,
            latest_record_id: Some(3),
            concept_record_id: Some(4),
            doi: Some("10.5281/zenodo.3".to_owned()),
            concept_doi: Some("10.5281/zenodo.1".to_owned()),
        }
    }

    #[test]
    fn parse_pubchem_record_accepts_tab_separated_rows() {
        let record = "2244\tCCO\n";
        let parsed = parse_pubchem_record(record).expect("record should parse");
        assert_eq!(parsed.0, "2244");
        assert_eq!(parsed.1, "CCO");
    }

    #[test]
    fn parse_pubchem_record_accepts_whitespace_fallback() {
        let record = "2244 CCO\r\n";
        let parsed = parse_pubchem_record(record).expect("record should parse");
        assert_eq!(parsed.0, "2244");
        assert_eq!(parsed.1, "CCO");
    }

    #[test]
    fn parse_pubchem_record_rejects_empty_rows() {
        assert!(parse_pubchem_record("\n").is_none());
    }

    #[test]
    fn publish_mode_labels_match_expected_values() {
        assert_eq!(PublishMode::Production.label(), "production");
        assert_eq!(PublishMode::Sandbox.label(), "sandbox");
        assert_eq!(PublishMode::Skip.label(), "skip");
    }

    #[test]
    fn pipeline_config_default_matches_fixed_binary_defaults() {
        let config = PipelineConfig::default();

        assert_eq!(config.pubchem_url, DEFAULT_PUBCHEM_SMILES_URL);
        assert_eq!(config.batch_size, DEFAULT_BATCH_SIZE);
        assert_eq!(config.parquet_batch_rows, DEFAULT_PARQUET_BATCH_ROWS);
        assert_eq!(config.input_mode, InputMode::DownloadAndDecompress);
        assert_eq!(config.publish_mode, PublishMode::Production);
        assert_eq!(
            config.infographic_path,
            PathBuf::from("results/pubchem-topology-infographic.svg")
        );
        assert_eq!(
            config.zenodo_state_path,
            PathBuf::from(".zenodo/production-state.toml")
        );
    }

    #[test]
    fn missing_publication_warning_is_emitted_for_production_without_token() {
        assert_eq!(
            missing_publication_warning(PublishMode::Production, false),
            Some("warning: ZENODO_TOKEN is not set; continuing without Zenodo publication")
        );
        assert_eq!(
            missing_publication_warning(PublishMode::Production, true),
            None
        );
        assert_eq!(
            missing_publication_warning(PublishMode::Sandbox, false),
            None
        );
        assert_eq!(missing_publication_warning(PublishMode::Skip, false), None);
    }

    #[test]
    fn build_zenodo_client_skips_production_without_token() -> Result<()> {
        let client = build_zenodo_client(PublishMode::Production, false)?;
        assert!(client.is_none());
        Ok(())
    }

    #[test]
    fn validate_config_rejects_zero_batch_size() {
        let config = PipelineConfig {
            batch_size: 0,
            ..PipelineConfig::default()
        };

        let error = validate_config(&config).expect_err("batch size should be rejected");
        assert!(error.to_string().contains("batch size"));
    }

    #[test]
    fn validate_config_rejects_zero_parquet_batch_rows() {
        let config = PipelineConfig {
            parquet_batch_rows: 0,
            ..PipelineConfig::default()
        };

        let error = validate_config(&config).expect_err("Parquet rows should be rejected");
        assert!(error.to_string().contains("Parquet batch rows"));
    }

    #[test]
    fn classify_smiles_marks_ethanol_as_tree_like_and_planar() {
        let smiles: Smiles = "CCO".parse().expect("valid SMILES");
        let metrics = classify_topology_smiles(&smiles).expect("classification should succeed");
        let checks = metrics.checks;

        assert_eq!(checks.len(), CHECK_COUNT);
        assert_eq!(metrics.connected_components, 1);
        assert_eq!(metrics.diameter, Some(2));
        assert_eq!(metrics.triangle_count, 0);
        assert_eq!(metrics.square_count, 0);
        assert_eq!(metrics.clustering_coefficient, 0.0);
        assert_eq!(metrics.square_clustering_coefficient, 0.0);
        assert!(checks[Check::Tree as usize]);
        assert!(checks[Check::Forest as usize]);
        assert!(checks[Check::Cactus as usize]);
        assert!(checks[Check::Chordal as usize]);
        assert!(checks[Check::Planar as usize]);
        assert!(checks[Check::Outerplanar as usize]);
        assert!(checks[Check::Bipartite as usize]);
        assert!(!checks[Check::K23Homeomorph as usize]);
        assert!(!checks[Check::K33Homeomorph as usize]);
        assert!(!checks[Check::K4Homeomorph as usize]);
    }

    #[test]
    fn classify_smiles_detects_k4_homeomorph_for_tetrahedrane_topology() {
        let smiles: Smiles = "*12*3*1*23".parse().expect("valid tetrahedrane topology");
        let metrics = classify_topology_smiles(&smiles).expect("classification should succeed");
        let checks = metrics.checks;

        assert_eq!(metrics.triangle_count, 4);
        assert!(checks[Check::Planar as usize]);
        assert!(!checks[Check::Outerplanar as usize]);
        assert!(checks[Check::K4Homeomorph as usize]);
    }

    #[test]
    fn classify_smiles_counts_cyclopropane_triangle() {
        let smiles: Smiles = "C1CC1".parse().expect("valid cyclopropane");
        let metrics = classify_topology_smiles(&smiles).expect("classification should succeed");

        assert_eq!(metrics.triangle_count, 1);
        assert_eq!(metrics.square_count, 0);
        assert_eq!(metrics.clustering_coefficient, 1.0);
    }

    #[test]
    fn classify_smiles_counts_cyclobutane_square() {
        let smiles: Smiles = "C1CCC1".parse().expect("valid cyclobutane");
        let metrics = classify_topology_smiles(&smiles).expect("classification should succeed");

        assert_eq!(metrics.triangle_count, 0);
        assert_eq!(metrics.square_count, 1);
        assert_eq!(metrics.clustering_coefficient, 0.0);
        assert_eq!(metrics.square_clustering_coefficient, 1.0);
    }

    #[test]
    fn classify_record_returns_default_for_unparseable_rows() {
        let row = classify_record("not a valid pubchem row");
        assert_eq!(row.cid, 0);
        assert_eq!(row.state, RowState::ParseError);
        assert_eq!(row.checks, [false; CHECK_COUNT]);
    }

    #[test]
    fn classify_record_returns_default_for_non_numeric_cid() {
        let row = classify_record("bad\tCCO");
        assert_eq!(row.cid, 0);
        assert_eq!(row.state, RowState::ParseError);
    }

    #[test]
    fn classify_record_marks_topology_failures_when_classifier_errors() {
        let row = classify_record_with("2244\tCCO", |_| Err(anyhow!("forced failure")));
        assert_eq!(row.cid, 2244);
        assert_eq!(row.state, RowState::TopologyError);
        assert_eq!(row.checks, [false; CHECK_COUNT]);
    }

    #[test]
    fn batch_stats_observe_counts_topology_errors() {
        let mut stats = BatchStats::default();
        let row = RowClassification {
            cid: 1,
            state: RowState::TopologyError,
            checks: [false; CHECK_COUNT],
            connected_components: 0,
            diameter: 0,
            triangle_count: 0,
            square_count: 0,
            clustering_coefficient: 0.0,
            square_clustering_coefficient: 0.0,
        };

        stats.observe(&row);

        assert_eq!(stats.total_records, 1);
        assert_eq!(stats.parsed_records, 1);
        assert_eq!(stats.topology_errors, 1);
    }

    #[test]
    fn estimate_result_memory_matches_columnar_layout() {
        let rows = 120_000_000_u64;
        let estimated = estimate_result_memory_bytes(rows);
        assert_eq!(estimated, 3_990_000_000);
    }

    #[test]
    fn format_gibibytes_uses_two_decimal_places() {
        assert_eq!(format_gibibytes(1_073_741_824), "1.00 GiB");
    }

    #[test]
    fn temporary_path_appends_suffix_before_tmp_extension() {
        let path = temporary_path(Path::new("results/out.json"), "summary");
        assert_eq!(path, PathBuf::from("results/out.json.summary.tmp"));
    }

    #[test]
    fn ensure_parent_dir_rejects_root_path() {
        let error = ensure_parent_dir(Path::new("/")).expect_err("root path should fail");
        assert!(error.to_string().contains("does not have a parent"));
    }

    #[test]
    fn progress_bars_can_be_constructed_for_all_modes() {
        let bytes_bar = new_bytes_progress_bar("download", 10);
        assert_eq!(bytes_bar.length(), Some(10));
        bytes_bar.finish_and_clear();

        let spinner_bar = new_bytes_progress_bar("download", 0);
        spinner_bar.finish_and_clear();

        let items_bar = new_items_progress_bar("write parquet", 5);
        assert_eq!(items_bar.length(), Some(5));
        items_bar.finish_and_clear();

        let spinner = new_spinner("publish");
        spinner.finish_and_clear();
    }

    #[test]
    fn count_pubchem_records_counts_file_without_terminal_newline() -> Result<()> {
        let tempdir = tempdir()?;
        let path = tempdir.path().join("rows.tsv");
        write_text(&path, "1\tC\n2\tCC")?;

        let rows = count_pubchem_records(&path)?;
        assert_eq!(rows, 2);
        Ok(())
    }

    #[test]
    fn count_pubchem_records_handles_empty_files() -> Result<()> {
        let tempdir = tempdir()?;
        let path = tempdir.path().join("rows.tsv");
        write_text(&path, "")?;

        let rows = count_pubchem_records(&path)?;
        assert_eq!(rows, 0);
        Ok(())
    }

    #[test]
    fn prepare_input_requires_existing_decompressed_file() {
        let tempdir = tempdir().expect("tempdir should be created");
        let config = PipelineConfig {
            compressed_path: tempdir.path().join("sample.gz"),
            decompressed_path: tempdir.path().join("sample.tsv"),
            infographic_path: tempdir.path().join("sample.svg"),
            input_mode: InputMode::UseExistingDecompressed,
            publish_mode: PublishMode::Skip,
            ..PipelineConfig::default()
        };

        let error = prepare_input(&config).expect_err("missing decompressed input should fail");
        assert!(
            error
                .to_string()
                .contains("expected existing decompressed input")
        );
    }

    #[test]
    fn prepare_input_uses_existing_decompressed_and_compressed_sizes() -> Result<()> {
        let tempdir = tempdir()?;
        let compressed_path = tempdir.path().join("sample.gz");
        let decompressed_path = tempdir.path().join("sample.tsv");

        fs::write(&compressed_path, write_gzip(SAMPLE_TSV)?)?;
        write_text(&decompressed_path, SAMPLE_TSV)?;

        let config = PipelineConfig {
            compressed_path: compressed_path.clone(),
            decompressed_path: decompressed_path.clone(),
            infographic_path: tempdir.path().join("sample.svg"),
            input_mode: InputMode::UseExistingDecompressed,
            publish_mode: PublishMode::Skip,
            ..PipelineConfig::default()
        };

        let input = prepare_input(&config)?;

        assert_eq!(
            input.compressed_bytes,
            fs::metadata(&compressed_path)?.len()
        );
        assert_eq!(
            input.decompressed_bytes,
            fs::metadata(&decompressed_path)?.len()
        );
        Ok(())
    }

    #[test]
    fn prepare_input_download_mode_reuses_existing_decompressed_without_downloading() -> Result<()>
    {
        let tempdir = tempdir()?;
        let decompressed_path = tempdir.path().join("sample.tsv");
        write_text(&decompressed_path, SAMPLE_TSV)?;

        let config = PipelineConfig {
            pubchem_url: "http://127.0.0.1:1/CID-SMILES.gz".to_owned(),
            compressed_path: tempdir.path().join("sample.gz"),
            decompressed_path: decompressed_path.clone(),
            infographic_path: tempdir.path().join("sample.svg"),
            input_mode: InputMode::DownloadAndDecompress,
            publish_mode: PublishMode::Skip,
            ..PipelineConfig::default()
        };

        let input = prepare_input(&config)?;

        assert_eq!(input.compressed_bytes, 0);
        assert_eq!(
            input.decompressed_bytes,
            fs::metadata(&decompressed_path)?.len()
        );
        assert_eq!(fs::read_to_string(&decompressed_path)?, SAMPLE_TSV);
        Ok(())
    }

    #[test]
    fn prepare_input_download_mode_reuses_existing_gzip_without_downloading() -> Result<()> {
        let tempdir = tempdir()?;
        let compressed_path = tempdir.path().join("sample.gz");
        let decompressed_path = tempdir.path().join("sample.tsv");
        fs::write(&compressed_path, write_gzip(SAMPLE_TSV)?)?;

        let config = PipelineConfig {
            pubchem_url: "http://127.0.0.1:1/CID-SMILES.gz".to_owned(),
            compressed_path: compressed_path.clone(),
            decompressed_path: decompressed_path.clone(),
            infographic_path: tempdir.path().join("sample.svg"),
            input_mode: InputMode::DownloadAndDecompress,
            publish_mode: PublishMode::Skip,
            ..PipelineConfig::default()
        };

        let input = prepare_input(&config)?;

        assert_eq!(
            input.compressed_bytes,
            fs::metadata(&compressed_path)?.len()
        );
        assert_eq!(
            input.decompressed_bytes,
            fs::metadata(&decompressed_path)?.len()
        );
        assert_eq!(fs::read_to_string(&decompressed_path)?, SAMPLE_TSV);
        Ok(())
    }

    #[test]
    fn run_with_config_downloads_and_processes_a_local_snapshot() -> Result<()> {
        let gzip = write_gzip(SAMPLE_TSV)?;
        let (url, handle) = spawn_http_server(gzip)?;
        let tempdir = tempdir()?;

        let config = PipelineConfig {
            pubchem_url: url,
            compressed_path: tempdir.path().join("data/CID-SMILES.gz"),
            decompressed_path: tempdir.path().join("data/CID-SMILES.tsv"),
            parquet_path: tempdir.path().join("results/pubchem-topology.parquet"),
            summary_path: tempdir.path().join("results/pubchem-topology-summary.json"),
            infographic_path: tempdir
                .path()
                .join("results/pubchem-topology-infographic.svg"),
            zenodo_state_path: tempdir.path().join(".zenodo/production-state.toml"),
            batch_size: 2,
            parquet_batch_rows: 2,
            input_mode: InputMode::DownloadAndDecompress,
            publish_mode: PublishMode::Skip,
        };

        let outcome = run_with_config(&config)?;
        handle.join().expect("server thread should not panic")?;

        assert_eq!(outcome.report.total_records, 4);
        assert_eq!(outcome.report.parsed_records, 3);
        assert_eq!(outcome.report.parse_errors, 1);
        assert_eq!(outcome.report.counts, [1, 1, 3, 1, 3, 3, 0, 0, 0, 3]);
        assert!(config.compressed_path.exists());
        assert!(config.decompressed_path.exists());
        assert!(config.parquet_path.exists());
        assert!(config.summary_path.exists());
        assert!(config.infographic_path.exists());
        Ok(())
    }

    #[test]
    fn classify_pubchem_smiles_processes_the_tail_batch() -> Result<()> {
        let tempdir = tempdir()?;
        let path = tempdir.path().join("sample.tsv");
        write_text(&path, "2244\tCCO\n240\tC1CCCCC1\nbad\tCCO")?;

        let (columns, stats) = classify_pubchem_smiles(&path, 2, 3)?;

        assert_eq!(stats.total_records, 3);
        assert_eq!(stats.parsed_records, 2);
        assert_eq!(stats.parse_errors, 1);
        assert_eq!(columns.len(), 3);
        Ok(())
    }

    #[test]
    fn parquet_schema_contains_expected_columns() {
        let schema = parquet_schema();
        let field_names = schema
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>();

        assert_eq!(field_names[0], "cid");
        assert_eq!(field_names[1], "connected_components");
        assert_eq!(field_names[2], "diameter");
        assert_eq!(field_names[3], "triangle_count");
        assert_eq!(field_names[4], "square_count");
        assert_eq!(field_names[5], "clustering_coefficient");
        assert_eq!(field_names[6], "square_clustering_coefficient");
        assert!(!field_names.contains(&"parse_ok"));
        assert!(!field_names.contains(&"topology_ok"));
        assert!(field_names.contains(&"k4_homeomorph"));
    }

    #[test]
    fn write_parquet_round_trips_boolean_columns() -> Result<()> {
        let tempdir = tempdir()?;
        let path = tempdir.path().join("rows.parquet");
        let mut columns = TopologyColumns::with_capacity(2);
        columns.extend(vec![
            RowClassification {
                cid: 1,
                state: RowState::Success,
                connected_components: 1,
                diameter: 2,
                triangle_count: 0,
                square_count: 0,
                clustering_coefficient: 0.0,
                square_clustering_coefficient: 0.0,
                checks: [
                    true, true, true, false, true, true, false, false, false, true,
                ],
            },
            RowClassification {
                cid: 2,
                state: RowState::ParseError,
                connected_components: 0,
                diameter: 0,
                triangle_count: 0,
                square_count: 0,
                clustering_coefficient: 0.0,
                square_clustering_coefficient: 0.0,
                checks: [false; CHECK_COUNT],
            },
        ]);

        write_parquet(&columns, &path, 1)?;

        let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(path)?)?;
        assert_eq!(reader.metadata().file_metadata().num_rows(), 2);
        Ok(())
    }

    #[test]
    fn write_summary_round_trips_json() -> Result<()> {
        let tempdir = tempdir()?;
        let path = tempdir.path().join("summary.json");
        let report = sample_report();

        write_summary(&report, &path)?;

        let decoded: TopologyReport = serde_json::from_str(&fs::read_to_string(path)?)?;
        assert_eq!(decoded.total_records, report.total_records);
        assert_eq!(decoded.check_names, report.check_names);
        Ok(())
    }

    #[test]
    fn load_zenodo_state_returns_none_for_missing_files() -> Result<()> {
        let tempdir = tempdir()?;
        let path = tempdir.path().join("missing.toml");

        assert!(load_zenodo_state(&path)?.is_none());
        Ok(())
    }

    #[test]
    fn write_and_load_zenodo_state_round_trip() -> Result<()> {
        let tempdir = tempdir()?;
        let path = tempdir.path().join("state.toml");
        let state = sample_state("sandbox");

        write_zenodo_state(&path, &state)?;
        let loaded = load_zenodo_state(&path)?.expect("state should load");
        let contents = fs::read_to_string(&path)?;

        assert_eq!(loaded.endpoint, "sandbox");
        assert_eq!(loaded.root_deposition_id, 1);
        assert!(contents.contains("Persistent Zenodo deposition state"));
        Ok(())
    }

    #[test]
    fn build_zenodo_metadata_contains_creator_and_keywords() -> Result<()> {
        let metadata = build_zenodo_metadata(&sample_report())?;

        assert_eq!(
            metadata.title,
            "PubChem CID-SMILES topology classification snapshot"
        );
        assert_eq!(metadata.creators.len(), 1);
        assert_eq!(
            metadata.creators[0].orcid.as_deref(),
            Some("0000-0002-1269-2038")
        );
        assert!(metadata.keywords.iter().any(|keyword| keyword == "PubChem"));
        Ok(())
    }

    #[test]
    fn publish_artifacts_rejects_endpoint_mismatch_before_network() -> Result<()> {
        let tempdir = tempdir()?;
        let state_path = tempdir.path().join("sandbox-state.toml");
        write_zenodo_state(&state_path, &sample_state("production"))?;

        let config = PipelineConfig {
            compressed_path: tempdir.path().join("data/sample.gz"),
            decompressed_path: tempdir.path().join("data/sample.tsv"),
            parquet_path: tempdir.path().join("results/sample.parquet"),
            summary_path: tempdir.path().join("results/sample.json"),
            infographic_path: tempdir.path().join("results/sample.svg"),
            zenodo_state_path: state_path,
            publish_mode: PublishMode::Sandbox,
            input_mode: InputMode::UseExistingDecompressed,
            ..PipelineConfig::default()
        };
        let client = ZenodoClient::new(Auth::new("token"))?;
        let runtime = tokio::runtime::Runtime::new()?;

        let error = runtime
            .block_on(publish_artifacts(&config, &sample_report(), &client))
            .expect_err("mismatched state should fail before publishing");
        assert!(
            error
                .to_string()
                .contains("targets production but this run uses sandbox")
        );
        Ok(())
    }
}
