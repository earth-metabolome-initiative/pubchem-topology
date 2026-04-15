//! End-to-end local fixture test for the fixed-flow pipeline.
//!
//! This test does not publish anything and exercises the local Parquet and
//! summary-writing path only.
//!
#![allow(
    clippy::expect_used,
    clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    clippy::panic,
    clippy::unwrap_used
)]

use std::{fs, path::Path};

use anyhow::Result;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use pubchem_topology::{InputMode, PipelineConfig, PublishMode, run_with_config};
use tempfile::tempdir;

fn copy_fixture(destination: &Path) -> Result<()> {
    fs::copy("tests/fixtures/sample_pubchem.tsv", destination)?;
    Ok(())
}

#[test]
fn writes_parquet_and_summary_from_existing_input() -> Result<()> {
    let tempdir = tempdir()?;
    let compressed_path = tempdir.path().join("CID-SMILES.gz");
    let decompressed_path = tempdir.path().join("CID-SMILES.tsv");
    let parquet_path = tempdir.path().join("pubchem-topology.parquet");
    let summary_path = tempdir.path().join("pubchem-topology-summary.json");
    let infographic_path = tempdir.path().join("pubchem-topology-infographic.svg");
    let zenodo_state_path = tempdir.path().join("production-state.toml");

    copy_fixture(&decompressed_path)?;

    let config = PipelineConfig {
        pubchem_url: "fixture://sample_pubchem.tsv".to_owned(),
        compressed_path,
        decompressed_path: decompressed_path.clone(),
        parquet_path: parquet_path.clone(),
        summary_path: summary_path.clone(),
        infographic_path: infographic_path.clone(),
        zenodo_state_path,
        batch_size: 2,
        parquet_batch_rows: 2,
        input_mode: InputMode::UseExistingDecompressed,
        publish_mode: PublishMode::Skip,
    };

    let outcome = run_with_config(&config)?;

    assert_eq!(outcome.report.total_records, 4);
    assert_eq!(outcome.report.parsed_records, 3);
    assert_eq!(outcome.report.parse_errors, 1);
    assert_eq!(outcome.report.topology_errors, 0);
    assert_eq!(outcome.report.counts, [1, 1, 3, 1, 3, 3, 0, 0, 0, 3]);
    assert_eq!(outcome.report.infographic_path, infographic_path);
    assert!(parquet_path.exists());
    assert!(summary_path.exists());
    assert!(infographic_path.exists());

    let summary = fs::read_to_string(summary_path)?;
    let infographic = fs::read_to_string(infographic_path)?;
    assert!(summary.contains("\"total_records\": 4"));
    assert!(summary.contains("\"bipartite\""));
    assert!(summary.contains("\"connected_components_histogram\""));
    assert!(summary.contains("\"diameter_histogram\""));
    assert!(summary.contains("\"pipeline_elapsed_seconds\""));
    assert!(summary.contains("\"infographic_path\""));
    assert!(infographic.contains("PubChem Molecular Topology"));
    assert!(infographic.contains("github.com/earth-metabolome-initiative/pubchem-topology"));
    assert!(infographic.contains("baseline-shift=\"-18%\""));

    let parquet_file = fs::File::open(parquet_path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(parquet_file)?;
    assert_eq!(builder.metadata().file_metadata().num_rows(), 4);

    Ok(())
}
