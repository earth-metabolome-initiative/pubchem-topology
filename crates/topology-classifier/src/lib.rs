//! Shared graph-topology classification for SMILES strings.
//!
//! This crate exposes a browser-safe classification API used both by the
//! batch PubChem pipeline and by the Dioxus web app.
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

use core::str::FromStr;

mod graphlets;

pub use graphlets::graphlet_svg;

use geometric_traits::traits::{
    BipartiteDetection, CactusDetection, ChordalDetection, Diameter, K4HomeomorphDetection,
    K23HomeomorphDetection, K33HomeomorphDetection, OuterplanarityDetection, PlanarityDetection,
    TreeDetection,
    algorithms::{
        LocalClusteringCoefficientScorer, NodeScorer, SquareClusteringCoefficientScorer,
        SquareCountScorer, TriangleCountScorer,
    },
};
pub use smiles_parser::smiles::Smiles;
use thiserror::Error;

/// Total number of emitted topology checks.
pub const CHECK_COUNT: usize = 10;

/// Boolean topology checks emitted per molecule.
#[repr(usize)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum Check {
    /// Whether the topology is a tree.
    Tree = 0,
    /// Whether the topology is a forest.
    Forest = 1,
    /// Whether the topology is a cactus graph.
    Cactus = 2,
    /// Whether the topology is chordal.
    Chordal = 3,
    /// Whether the topology is planar.
    Planar = 4,
    /// Whether the topology is outerplanar.
    Outerplanar = 5,
    /// Whether the topology contains a `K2,3` homeomorph.
    K23Homeomorph = 6,
    /// Whether the topology contains a `K3,3` homeomorph.
    K33Homeomorph = 7,
    /// Whether the topology contains a `K4` homeomorph.
    K4Homeomorph = 8,
    /// Whether the topology is bipartite.
    Bipartite = 9,
}

impl Check {
    /// Checks in the same order used by the count array and Parquet columns.
    pub const ALL: [Self; CHECK_COUNT] = [
        Self::Tree,
        Self::Forest,
        Self::Cactus,
        Self::Chordal,
        Self::Planar,
        Self::Outerplanar,
        Self::K23Homeomorph,
        Self::K33Homeomorph,
        Self::K4Homeomorph,
        Self::Bipartite,
    ];

    /// Stable column and count label for the check.
    pub const fn name(self) -> &'static str {
        match self {
            Self::Tree => "tree",
            Self::Forest => "forest",
            Self::Cactus => "cactus",
            Self::Chordal => "chordal",
            Self::Planar => "planar",
            Self::Outerplanar => "outerplanar",
            Self::K23Homeomorph => "k23_homeomorph",
            Self::K33Homeomorph => "k33_homeomorph",
            Self::K4Homeomorph => "k4_homeomorph",
            Self::Bipartite => "bipartite",
        }
    }

    /// Human-friendly label for UI rendering.
    pub const fn label(self) -> &'static str {
        match self {
            Self::Tree => "Tree",
            Self::Forest => "Forest",
            Self::Cactus => "Cactus",
            Self::Chordal => "Chordal",
            Self::Planar => "Planar",
            Self::Outerplanar => "Outerplanar",
            Self::K23Homeomorph => "K2,3 homeomorph",
            Self::K33Homeomorph => "K3,3 homeomorph",
            Self::K4Homeomorph => "K4 homeomorph",
            Self::Bipartite => "Bipartite",
        }
    }

    /// Short explanatory text for the check.
    pub const fn description(self) -> &'static str {
        match self {
            Self::Tree => "Connected and acyclic.",
            Self::Forest => "Acyclic, but not necessarily connected.",
            Self::Cactus => "Each edge lies on at most one simple cycle.",
            Self::Chordal => "Every cycle of length at least four has a chord.",
            Self::Planar => "Embeddable in the plane without crossings.",
            Self::Outerplanar => "Planar with all vertices on the outer face.",
            Self::K23Homeomorph => "Two branch vertices each linked to three others.",
            Self::K33Homeomorph => "Three branch vertices per side, with links between sides.",
            Self::K4Homeomorph => "Four branch vertices all linked to one another.",
            Self::Bipartite => "Contains no odd cycle.",
        }
    }

    /// Whether the check is an obstruction-detection predicate.
    pub const fn is_obstruction(self) -> bool {
        matches!(
            self,
            Self::K23Homeomorph | Self::K33Homeomorph | Self::K4Homeomorph
        )
    }
}

/// Topology data derived from a SMILES graph.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct TopologyClassification {
    /// Number of connected components in the molecular graph.
    pub connected_components: u16,
    /// Graph diameter for connected molecules. `None` for disconnected graphs.
    pub diameter: Option<u16>,
    /// Number of distinct triangles in the molecular graph.
    pub triangle_count: u32,
    /// Number of distinct 4-cycles in the molecular graph.
    pub square_count: u32,
    /// Mean local clustering coefficient across all graph nodes.
    pub clustering_coefficient: f64,
    /// Mean square clustering coefficient across all graph nodes.
    pub square_clustering_coefficient: f64,
    /// Topology predicates in `Check::ALL` order.
    pub checks: [bool; CHECK_COUNT],
}

impl TopologyClassification {
    /// Returns the boolean value of a specific topology predicate.
    pub const fn check(&self, check: Check) -> bool {
        self.checks[check as usize]
    }
}

/// One non-empty SMILES line submitted for batch classification.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct BatchInputLine {
    /// Original 1-based line number in the submitted batch.
    pub line_number: usize,
    /// Trimmed SMILES string to classify.
    pub smiles: String,
}

/// One classified SMILES batch entry.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct BatchEntry {
    /// Original 1-based line number in the submitted batch.
    pub line_number: usize,
    /// Trimmed SMILES string that was classified.
    pub smiles: String,
    /// Either the topology classification or the parsing/classification error.
    pub result: Result<TopologyClassification, String>,
}

/// Command sent from the UI thread to the topology worker.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum WorkerBatchRequest {
    /// Starts classifying a fresh SMILES batch.
    Classify {
        /// Monotonic request token used to ignore stale responses.
        token: u64,
        /// Non-empty SMILES lines to classify.
        lines: Vec<BatchInputLine>,
    },
    /// Stops any in-flight batch as soon as the worker yields.
    Cancel {
        /// Replacement token that invalidates the previous request.
        token: u64,
    },
}

impl WorkerBatchRequest {
    /// Returns the monotonic token carried by the request.
    pub const fn token(&self) -> u64 {
        match self {
            Self::Classify { token, .. } | Self::Cancel { token } => *token,
        }
    }
}

/// Response sent from the topology worker back to the UI thread.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum WorkerBatchResponse {
    /// Partial progress update for the current batch.
    Progress {
        /// Request token being reported.
        token: u64,
        /// Number of completed lines so far.
        completed: usize,
        /// Total lines in the batch.
        total: usize,
    },
    /// Final classified batch.
    Complete {
        /// Request token being reported.
        token: u64,
        /// Fully classified batch entries.
        entries: Vec<BatchEntry>,
    },
    /// Fatal worker-side failure.
    Fatal {
        /// Request token being reported.
        token: u64,
        /// Human-readable failure message.
        message: String,
    },
}

impl WorkerBatchResponse {
    /// Returns the request token carried by the response.
    pub const fn token(&self) -> u64 {
        match self {
            Self::Progress { token, .. }
            | Self::Complete { token, .. }
            | Self::Fatal { token, .. } => *token,
        }
    }
}

/// Errors that can happen while parsing or classifying a molecular graph.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ClassificationError {
    /// The input string was not valid SMILES.
    #[error("failed to parse SMILES: {0}")]
    Parse(String),
    /// The number of connected components did not fit into `u16`.
    #[error("too many connected components to fit in u16")]
    ConnectedComponentsOverflow,
    /// The graph diameter did not fit into `u16`.
    #[error("diameter does not fit in u16")]
    DiameterOverflow,
    /// The graph triangle count did not fit into `u32`.
    #[error("triangle count does not fit in u32")]
    TriangleCountOverflow,
    /// The graph square count did not fit into `u32`.
    #[error("square count does not fit in u32")]
    SquareCountOverflow,
    /// Diameter computation failed inside the graph library.
    #[error("diameter failed: {0}")]
    Diameter(String),
    /// Planarity detection failed inside the graph library.
    #[error("planarity detection failed: {0}")]
    Planarity(String),
    /// Outerplanarity detection failed inside the graph library.
    #[error("outerplanarity detection failed: {0}")]
    Outerplanarity(String),
    /// K2,3 detection failed inside the graph library.
    #[error("K2,3 detection failed: {0}")]
    K23(String),
    /// K3,3 detection failed inside the graph library.
    #[error("K3,3 detection failed: {0}")]
    K33(String),
    /// K4 detection failed inside the graph library.
    #[error("K4 detection failed: {0}")]
    K4(String),
}

/// Parses and classifies one SMILES string.
///
/// # Errors
///
/// Returns an error if the SMILES string is invalid or if one of the topology
/// algorithms fails.
pub fn classify_smiles_text(
    smiles_text: &str,
) -> Result<TopologyClassification, ClassificationError> {
    let smiles = Smiles::from_str(smiles_text)
        .map_err(|error| ClassificationError::Parse(error.to_string()))?;
    classify_smiles(&smiles)
}

/// Classifies one already-trimmed SMILES batch line.
pub fn classify_batch_line(line: BatchInputLine) -> BatchEntry {
    let BatchInputLine {
        line_number,
        smiles,
    } = line;

    BatchEntry {
        line_number,
        result: classify_smiles_text(&smiles).map_err(|error| error.to_string()),
        smiles,
    }
}

/// Classifies an already parsed molecular graph.
///
/// # Errors
///
/// Returns an error if one of the topology algorithms fails or if a metric does
/// not fit in the chosen serialized representation.
pub fn classify_smiles(smiles: &Smiles) -> Result<TopologyClassification, ClassificationError> {
    let connected_components = u16::try_from(smiles.connected_components().number_of_components())
        .map_err(|_| ClassificationError::ConnectedComponentsOverflow)?;
    let diameter = if connected_components == 1 {
        Some(
            u16::try_from(
                smiles
                    .diameter()
                    .map_err(|error| ClassificationError::Diameter(error.to_string()))?,
            )
            .map_err(|_| ClassificationError::DiameterOverflow)?,
        )
    } else {
        None
    };
    let triangle_count = aggregate_triangle_count(smiles)?;
    let square_count = aggregate_square_count(smiles)?;
    let clustering_coefficient =
        average_score(&LocalClusteringCoefficientScorer.score_nodes(smiles));
    let square_clustering_coefficient =
        average_score(&SquareClusteringCoefficientScorer.score_nodes(smiles));

    let is_tree = smiles.is_tree();
    let is_forest = if is_tree { true } else { smiles.is_forest() };
    let is_cactus = if is_forest { true } else { smiles.is_cactus() };
    let is_chordal = smiles.is_chordal();
    let is_planar = smiles
        .is_planar()
        .map_err(|error| ClassificationError::Planarity(error.to_string()))?;
    let is_outerplanar = if is_planar {
        smiles
            .is_outerplanar()
            .map_err(|error| ClassificationError::Outerplanarity(error.to_string()))?
    } else {
        false
    };
    let has_k23_homeomorph = if is_outerplanar {
        false
    } else {
        smiles
            .has_k23_homeomorph()
            .map_err(|error| ClassificationError::K23(error.to_string()))?
    };
    let has_k33_homeomorph = if is_planar {
        false
    } else {
        smiles
            .has_k33_homeomorph()
            .map_err(|error| ClassificationError::K33(error.to_string()))?
    };
    let has_k4_homeomorph = if is_outerplanar {
        false
    } else {
        smiles
            .has_k4_homeomorph()
            .map_err(|error| ClassificationError::K4(error.to_string()))?
    };
    let is_bipartite = smiles.is_bipartite();

    Ok(TopologyClassification {
        connected_components,
        diameter,
        triangle_count,
        square_count,
        clustering_coefficient,
        square_clustering_coefficient,
        checks: [
            is_tree,
            is_forest,
            is_cactus,
            is_chordal,
            is_planar,
            is_outerplanar,
            has_k23_homeomorph,
            has_k33_homeomorph,
            has_k4_homeomorph,
            is_bipartite,
        ],
    })
}

fn aggregate_triangle_count(smiles: &Smiles) -> Result<u32, ClassificationError> {
    u32::try_from(
        TriangleCountScorer::default()
            .score_nodes(smiles)
            .into_iter()
            .sum::<usize>()
            / 3,
    )
    .map_err(|_| ClassificationError::TriangleCountOverflow)
}

fn aggregate_square_count(smiles: &Smiles) -> Result<u32, ClassificationError> {
    u32::try_from(
        SquareCountScorer::default()
            .score_nodes(smiles)
            .into_iter()
            .sum::<usize>()
            / 4,
    )
    .map_err(|_| ClassificationError::SquareCountOverflow)
}

fn average_score(scores: &[f64]) -> f64 {
    if scores.is_empty() {
        return 0.0;
    }
    let mean = scores.iter().sum::<f64>() / scores.len() as f64;
    (mean * 1.0e12).round() / 1.0e12
}

#[cfg(test)]
mod tests {
    use super::{Check, classify_smiles_text};

    #[test]
    fn classifies_ethanol() {
        let classification = classify_smiles_text("CCO").expect("CCO should classify");

        assert_eq!(classification.connected_components, 1);
        assert_eq!(classification.diameter, Some(2));
        assert_eq!(classification.triangle_count, 0);
        assert_eq!(classification.square_count, 0);
        assert_eq!(classification.clustering_coefficient, 0.0);
        assert_eq!(classification.square_clustering_coefficient, 0.0);
        assert!(classification.check(Check::Tree));
        assert!(classification.check(Check::Forest));
        assert!(classification.check(Check::Cactus));
        assert!(classification.check(Check::Chordal));
        assert!(classification.check(Check::Planar));
        assert!(classification.check(Check::Outerplanar));
        assert!(classification.check(Check::Bipartite));
        assert!(!classification.check(Check::K23Homeomorph));
        assert!(!classification.check(Check::K33Homeomorph));
        assert!(!classification.check(Check::K4Homeomorph));
    }

    #[test]
    fn classifies_tetrahedrane_topology() {
        let classification =
            classify_smiles_text("*12*3*1*23").expect("tetrahedrane topology should classify");

        assert_eq!(classification.triangle_count, 4);
        assert!(classification.check(Check::Planar));
        assert!(!classification.check(Check::Outerplanar));
        assert!(classification.check(Check::K4Homeomorph));
    }

    #[test]
    fn classifies_cyclopropane_motifs() {
        let classification = classify_smiles_text("C1CC1").expect("cyclopropane should classify");

        assert_eq!(classification.triangle_count, 1);
        assert_eq!(classification.square_count, 0);
        assert_eq!(classification.clustering_coefficient, 1.0);
    }

    #[test]
    fn classifies_cyclobutane_square_metrics() {
        let classification = classify_smiles_text("C1CCC1").expect("cyclobutane should classify");

        assert_eq!(classification.triangle_count, 0);
        assert_eq!(classification.square_count, 1);
        assert_eq!(classification.clustering_coefficient, 0.0);
        assert_eq!(classification.square_clustering_coefficient, 1.0);
    }
}
