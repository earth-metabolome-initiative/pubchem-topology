//! Binary entrypoint for the fixed-flow PubChem topology pipeline.
//!
//! The actual work lives in the library so the output formatting can be tested
//! without invoking the full production run.
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

use std::io::{self, Write};

use anyhow::Result;
use pubchem_topology::{RunOutcome, run};

fn main() -> Result<()> {
    let outcome = run()?;
    write_outcome(io::stdout().lock(), &outcome)
}

fn write_outcome(mut writer: impl Write, outcome: &RunOutcome) -> Result<()> {
    for line in outcome_lines(outcome) {
        writeln!(writer, "{line}")?;
    }

    Ok(())
}

fn outcome_lines(outcome: &RunOutcome) -> Vec<String> {
    let mut lines = vec![
        format!("total records    {}", outcome.report.total_records),
        format!("parsed records   {}", outcome.report.parsed_records),
        format!("parse errors     {}", outcome.report.parse_errors),
        format!("topology errors  {}", outcome.report.topology_errors),
        format!(
            "runtime          {}",
            format_elapsed_seconds(outcome.report.pipeline_elapsed_seconds)
        ),
        format!(
            "memory estimate  {}",
            outcome.report.estimated_result_memory_bytes
        ),
        format!("parquet path     {}", outcome.report.parquet_path.display()),
        format!("summary path     {}", outcome.report.summary_path.display()),
        format!(
            "svg path         {}",
            outcome.report.infographic_path.display()
        ),
    ];

    for (name, count) in outcome.report.check_names.iter().zip(outcome.report.counts) {
        lines.push(format!("{name:16} {count}"));
    }

    if let Some(publication) = &outcome.publication {
        lines.push(format!("zenodo endpoint  {}", publication.endpoint));
        lines.push(format!(
            "root deposit     {}",
            publication.root_deposition_id
        ));
        lines.push(format!(
            "latest deposit   {}",
            publication.latest_deposition_id
        ));
        if let Some(record_id) = publication.record_id {
            lines.push(format!("record id        {record_id}"));
        }
        if let Some(doi) = &publication.doi {
            lines.push(format!("doi              {doi}"));
        }
    }

    lines
}

fn format_elapsed_seconds(seconds: f64) -> String {
    let total_seconds = seconds.max(0.0).round() as u64;
    let hours = total_seconds / 3_600;
    let minutes = (total_seconds % 3_600) / 60;
    let seconds = total_seconds % 60;

    if hours > 0 {
        return format!("{hours}h {minutes:02}m {seconds:02}s");
    }
    if minutes > 0 {
        return format!("{minutes}m {seconds:02}s");
    }
    format!("{seconds}s")
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::{RunOutcome, outcome_lines};
    use pubchem_topology::{
        CoefficientHistogramBin, ComponentHistogramBin, ScalarHistogramBin, TopologyReport,
        ZenodoPublicationSummary,
    };

    fn sample_report() -> TopologyReport {
        TopologyReport {
            source_url: "fixture://sample".to_owned(),
            compressed_path: PathBuf::from("data/sample.gz"),
            decompressed_path: PathBuf::from("data/sample.tsv"),
            parquet_path: PathBuf::from("results/sample.parquet"),
            summary_path: PathBuf::from("results/sample.json"),
            infographic_path: PathBuf::from("results/sample.svg"),
            compressed_bytes: 10,
            decompressed_bytes: 20,
            total_records: 4,
            parsed_records: 3,
            parse_errors: 1,
            topology_errors: 0,
            pipeline_elapsed_seconds: 97.0,
            estimated_result_memory_bytes: 64,
            check_names: vec![
                "tree".to_owned(),
                "forest".to_owned(),
                "cactus".to_owned(),
                "chordal".to_owned(),
                "planar".to_owned(),
                "outerplanar".to_owned(),
                "k23_homeomorph".to_owned(),
                "k33_homeomorph".to_owned(),
                "k4_homeomorph".to_owned(),
                "bipartite".to_owned(),
            ],
            counts: [1, 1, 1, 0, 1, 1, 0, 0, 0, 1],
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

    #[test]
    fn outcome_lines_render_report_only_output() {
        let outcome = RunOutcome {
            report: sample_report(),
            publication: None,
        };

        let lines = outcome_lines(&outcome);

        assert!(lines.iter().any(|line| line == "total records    4"));
        assert!(
            lines
                .iter()
                .any(|line| line == "summary path     results/sample.json")
        );
        assert!(lines.iter().any(|line| line == "runtime          1m 37s"));
        assert!(
            lines
                .iter()
                .any(|line| line == "svg path         results/sample.svg")
        );
        assert!(lines.iter().any(|line| line == "tree             1"));
        assert!(!lines.iter().any(|line| line.starts_with("zenodo endpoint")));
    }

    #[test]
    fn outcome_lines_render_publication_fields_when_present() {
        let outcome = RunOutcome {
            report: sample_report(),
            publication: Some(ZenodoPublicationSummary {
                endpoint: "sandbox".to_owned(),
                root_deposition_id: 11,
                latest_deposition_id: 12,
                record_id: Some(13),
                concept_record_id: Some(14),
                doi: Some("10.5281/zenodo.13".to_owned()),
                concept_doi: Some("10.5281/zenodo.11".to_owned()),
            }),
        };

        let lines = outcome_lines(&outcome);

        assert!(lines.iter().any(|line| line == "zenodo endpoint  sandbox"));
        assert!(lines.iter().any(|line| line == "root deposit     11"));
        assert!(lines.iter().any(|line| line == "latest deposit   12"));
        assert!(lines.iter().any(|line| line == "record id        13"));
        assert!(
            lines
                .iter()
                .any(|line| line == "doi              10.5281/zenodo.13")
        );
    }
}
