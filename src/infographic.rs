use std::{
    fs::{self, File},
    io::{BufWriter, Write},
    path::Path,
    process::Command,
};

use anyhow::{Context, Result};
use time::OffsetDateTime;
use topology_classifier::graphlet_svg;

use crate::{
    CHECK_COUNT, Check, CoefficientHistogramBin, ComponentHistogramBin, ScalarHistogramBin,
    TopologyReport, temporary_path,
};

const CANVAS_WIDTH: usize = 1_600;
const CANVAS_HEIGHT: usize = 3_042;
const PAGE_MARGIN: usize = 64;
const CONTENT_WIDTH: usize = CANVAS_WIDTH - PAGE_MARGIN * 2;
const SUMMARY_Y: usize = 250;
const SUMMARY_HEIGHT: usize = 194;
const ABSTRACT_WIDTH: usize = 1_356;
const ABSTRACT_FONT_SIZE: f32 = 18.0;
const ABSTRACT_LINE_HEIGHT: usize = 23;
const CARD_WIDTH: usize = 720;
const CARD_HEIGHT: usize = 272;
const CARD_GAP_X: usize = 32;
const CARD_GAP_Y: usize = 28;
const HISTOGRAM_SECTION_Y: usize = 472;
const HISTOGRAM_PANEL_HEIGHT: usize = 146;
const HISTOGRAM_PANEL_WIDTH: usize = CARD_WIDTH;
const HISTOGRAM_PANEL_GAP: usize = CARD_GAP_X;
const HISTOGRAM_ROW_GAP: usize = CARD_GAP_Y;
const DAG_SECTION_Y: usize = 998;
const DAG_PANEL_HEIGHT: usize = 390;
const DAG_GRAPH_OFFSET_X: usize = 17;
const DAG_GRAPH_OFFSET_Y: usize = 12;
const DAG_LEGEND_X: usize = 1_148;
const DAG_LEGEND_Y: usize = 44;
const CARD_START_Y: usize = 1_416;
const GRAPHLET_FRAME_X: usize = 22;
const GRAPHLET_FRAME_Y: usize = 42;
const GRAPHLET_FRAME_SIZE: usize = 160;
const GRAPHLET_VIEWBOX_SIZE: f32 = 160.0;
const GRAPHLET_SCALE_FACTOR: f32 = 0.86;
const CARD_TEXT_X: usize = 196;
const CARD_BAR_WIDTH: f64 = 470.0;
const CARD_DESCRIPTION_WIDTH: f32 = 474.0;
const FOOTER_Y: usize = 2_934;
const REPOSITORY_URL: &str = env!("CARGO_PKG_REPOSITORY");

struct CardSpec {
    check: Check,
    title: &'static str,
    description: &'static str,
    accent: &'static str,
    graphlet: &'static str,
    graphlet_shift_x: i32,
    graphlet_shift_y: i32,
    graphlet_scale: f32,
}

struct DagNodeSpec {
    check: Check,
    x: usize,
    y: usize,
    width: usize,
    height: usize,
}

const CARD_SPECS: [CardSpec; CHECK_COUNT] = [
    CardSpec {
        check: Check::Tree,
        title: "Tree",
        description: "Connected and acyclic, with n-1 edges and one simple path between any two vertices.",
        accent: "#2F6F65",
        graphlet: graphlet_svg(Check::Tree),
        graphlet_shift_x: 0,
        graphlet_shift_y: -14,
        graphlet_scale: 1.12,
    },
    CardSpec {
        check: Check::Forest,
        title: "Forest",
        description: "A disjoint union of trees: acyclic overall, but not required to form one connected component.",
        accent: "#5A8F42",
        graphlet: graphlet_svg(Check::Forest),
        graphlet_shift_x: 0,
        graphlet_shift_y: 10,
        graphlet_scale: 1.12,
    },
    CardSpec {
        check: Check::Cactus,
        title: "Cactus",
        description: "Each edge lies on at most one simple cycle, so cycles can meet only at articulation vertices.",
        accent: "#C46A2D",
        graphlet: graphlet_svg(Check::Cactus),
        graphlet_shift_x: 0,
        graphlet_shift_y: -10,
        graphlet_scale: 1.10,
    },
    CardSpec {
        check: Check::Chordal,
        title: "Chordal",
        description: "Every cycle of length at least four has a chord, excluding induced long cycles.",
        accent: "#A5503A",
        graphlet: graphlet_svg(Check::Chordal),
        graphlet_shift_x: 0,
        graphlet_shift_y: 0,
        graphlet_scale: 1.16,
    },
    CardSpec {
        check: Check::Planar,
        title: "Planar",
        description: "Can be embedded in the plane with no edge crossings after redrawing if needed.",
        accent: "#1B7288",
        graphlet: graphlet_svg(Check::Planar),
        graphlet_shift_x: 0,
        graphlet_shift_y: 0,
        graphlet_scale: 1.10,
    },
    CardSpec {
        check: Check::Outerplanar,
        title: "Outerplanar",
        description: "Planar and drawable with every vertex on the outer face.",
        accent: "#0E9683",
        graphlet: graphlet_svg(Check::Outerplanar),
        graphlet_shift_x: 0,
        graphlet_shift_y: 0,
        graphlet_scale: 1.10,
    },
    CardSpec {
        check: Check::K23Homeomorph,
        title: "K2,3 Homeomorph",
        description: "Contains a subdivision of K2,3, a classical obstruction to outerplanarity.",
        accent: "#A35C17",
        graphlet: graphlet_svg(Check::K23Homeomorph),
        graphlet_shift_x: 0,
        graphlet_shift_y: 0,
        graphlet_scale: 1.08,
    },
    CardSpec {
        check: Check::K33Homeomorph,
        title: "K3,3 Homeomorph",
        description: "Contains a subdivision of K3,3, a Kuratowski obstruction proving nonplanarity.",
        accent: "#B13D44",
        graphlet: graphlet_svg(Check::K33Homeomorph),
        graphlet_shift_x: 0,
        graphlet_shift_y: 0,
        graphlet_scale: 1.08,
    },
    CardSpec {
        check: Check::K4Homeomorph,
        title: "K4 Homeomorph",
        description: "Contains a subdivision of K4, another standard obstruction to outerplanarity.",
        accent: "#7D3A1B",
        graphlet: graphlet_svg(Check::K4Homeomorph),
        graphlet_shift_x: 0,
        graphlet_shift_y: 10,
        graphlet_scale: 1.10,
    },
    CardSpec {
        check: Check::Bipartite,
        title: "Bipartite",
        description: "Vertices split into two partitions, with edges only across the split and no odd cycle.",
        accent: "#3856A6",
        graphlet: graphlet_svg(Check::Bipartite),
        graphlet_shift_x: 0,
        graphlet_shift_y: 0,
        graphlet_scale: 1.16,
    },
];

const DAG_TOPOLOGY_NODES: [DagNodeSpec; 7] = [
    DagNodeSpec {
        check: Check::Tree,
        x: 92,
        y: 164,
        width: 136,
        height: 64,
    },
    DagNodeSpec {
        check: Check::Forest,
        x: 326,
        y: 164,
        width: 176,
        height: 64,
    },
    DagNodeSpec {
        check: Check::Cactus,
        x: 590,
        y: 164,
        width: 176,
        height: 64,
    },
    DagNodeSpec {
        check: Check::Chordal,
        x: 324,
        y: 72,
        width: 180,
        height: 64,
    },
    DagNodeSpec {
        check: Check::Bipartite,
        x: 310,
        y: 256,
        width: 208,
        height: 64,
    },
    DagNodeSpec {
        check: Check::Outerplanar,
        x: 878,
        y: 164,
        width: 216,
        height: 64,
    },
    DagNodeSpec {
        check: Check::Planar,
        x: 1_190,
        y: 164,
        width: 156,
        height: 64,
    },
];

const DAG_OBSTRUCTION_NODES: [DagNodeSpec; 3] = [
    DagNodeSpec {
        check: Check::K23Homeomorph,
        x: 882,
        y: 298,
        width: 104,
        height: 46,
    },
    DagNodeSpec {
        check: Check::K4Homeomorph,
        x: 998,
        y: 298,
        width: 80,
        height: 46,
    },
    DagNodeSpec {
        check: Check::K33Homeomorph,
        x: 1_216,
        y: 298,
        width: 104,
        height: 46,
    },
];

const DAG_DIRECT_IMPLICATIONS: [(Check, Check); 6] = [
    (Check::Tree, Check::Forest),
    (Check::Forest, Check::Cactus),
    (Check::Forest, Check::Chordal),
    (Check::Forest, Check::Bipartite),
    (Check::Cactus, Check::Outerplanar),
    (Check::Outerplanar, Check::Planar),
];

const DAG_EXCLUSIONS: [(Check, Check); 3] = [
    (Check::K23Homeomorph, Check::Outerplanar),
    (Check::K4Homeomorph, Check::Outerplanar),
    (Check::K33Homeomorph, Check::Planar),
];

pub(super) fn write_infographic(report: &TopologyReport, path: &Path) -> Result<()> {
    let progress_bar = crate::new_spinner("write svg");
    let svg = infographic_svg(report)?;
    let temp_path = temporary_path(path, "infographic");
    let mut writer = BufWriter::new(
        File::create(&temp_path)
            .with_context(|| format!("failed to create {}", temp_path.display()))?,
    );
    writer
        .write_all(svg.as_bytes())
        .with_context(|| format!("failed to write {}", temp_path.display()))?;
    writer
        .flush()
        .with_context(|| format!("failed to flush {}", temp_path.display()))?;
    fs::rename(&temp_path, path).with_context(|| {
        format!(
            "failed to move infographic from {} to {}",
            temp_path.display(),
            path.display()
        )
    })?;
    progress_bar.finish_with_message(format!("write svg      {}", path.display()));
    Ok(())
}

fn infographic_svg(report: &TopologyReport) -> Result<String> {
    let evaluated_records = report.parsed_records.saturating_sub(report.topology_errors);
    let max_count = report.counts.into_iter().max().unwrap_or(0);
    let abstract_text = build_abstract_text(report, evaluated_records);
    let subtitle_lines = wrap_text(
        "Graph-theoretic annotations for the current PubChem CID-SMILES release.",
        980.0,
        22.0,
    );
    let accessibility_desc = format!(
        "Infographic summarizing {} PubChem records, with {} parsed molecules, {} parse errors, {} topology errors, an implication map for the topology predicates, and a card for each graph predicate.",
        format_count(report.total_records),
        format_count(report.parsed_records),
        format_count(report.parse_errors),
        format_count(report.topology_errors),
    );

    let mut svg = String::new();
    svg.push_str(&format!(
        r##"<svg xmlns="http://www.w3.org/2000/svg" version="1.1" width="{CANVAS_WIDTH}" height="{CANVAS_HEIGHT}" viewBox="0 0 {CANVAS_WIDTH} {CANVAS_HEIGHT}" preserveAspectRatio="xMidYMid meet" role="img" aria-labelledby="title desc">
<title id="title">PubChem Molecular Topology</title>
<desc id="desc">{}</desc>
<defs>
  <linearGradient id="page-gradient" x1="0" y1="0" x2="1" y2="1">
    <stop offset="0%" stop-color="#F5EFE4"/>
    <stop offset="55%" stop-color="#FBF8F2"/>
    <stop offset="100%" stop-color="#EEF5F3"/>
  </linearGradient>
  <linearGradient id="hero-gradient" x1="0" y1="0" x2="1" y2="0">
    <stop offset="0%" stop-color="#1F766F"/>
    <stop offset="48%" stop-color="#1B7288"/>
    <stop offset="100%" stop-color="#C46A2D"/>
  </linearGradient>
  <filter id="card-shadow" x="-20%" y="-20%" width="140%" height="140%">
    <feDropShadow dx="0" dy="10" stdDeviation="16" flood-color="#334E68" flood-opacity="0.10"/>
  </filter>
  <marker id="dag-arrow" markerWidth="12" markerHeight="12" refX="10" refY="6" orient="auto" markerUnits="userSpaceOnUse">
    <path d="M 0 0 L 12 6 L 0 12 z" fill="#52606D"/>
  </marker>
  <marker id="dag-arrow-alert" markerWidth="12" markerHeight="12" refX="10" refY="6" orient="auto" markerUnits="userSpaceOnUse">
    <path d="M 0 0 L 12 6 L 0 12 z" fill="#B13D44"/>
  </marker>
  <style>
    text {{
      font-family: 'IBM Plex Sans', 'Source Sans 3', 'Segoe UI', sans-serif;
      fill: #1F2933;
    }}
    .eyebrow {{ font-size: 18px; font-weight: 700; letter-spacing: 0.16em; text-transform: uppercase; fill: #7B8794; }}
    .hero-title {{ font-size: 52px; font-weight: 760; letter-spacing: -0.03em; fill: #102A43; }}
    .hero-subtitle {{ font-size: 22px; fill: #52606D; }}
    .section-title {{ font-size: 20px; font-weight: 700; letter-spacing: 0.08em; text-transform: uppercase; fill: #52606D; }}
    .body {{ font-size: 22px; fill: #243B53; }}
    .body-small {{ font-size: 20px; fill: #334E68; }}
    .abstract {{ font-size: 18px; fill: #334E68; }}
    .muted {{ font-size: 18px; fill: #7B8794; }}
    .panel-title {{ font-size: 22px; font-weight: 720; fill: #102A43; }}
    .panel-note {{ font-size: 15px; fill: #7B8794; }}
    .dag-label {{ font-size: 20px; font-weight: 720; fill: #102A43; }}
    .hist-label {{ font-size: 15px; fill: #52606D; }}
    .hist-value {{ font-size: 14px; fill: #52606D; }}
    .card-title {{ font-size: 30px; font-weight: 760; fill: #102A43; }}
    .card-count {{ font-size: 42px; font-weight: 760; fill: #102A43; }}
    .card-share {{ font-size: 18px; fill: #52606D; }}
    .card-body {{ font-size: 18px; fill: #334E68; }}
    .footer-title {{ font-size: 28px; font-weight: 760; fill: #102A43; }}
    .footer-body {{ font-size: 18px; fill: #334E68; }}
    .mono {{ font-family: 'IBM Plex Mono', 'SFMono-Regular', monospace; font-size: 18px; fill: #334E68; }}
  </style>
</defs>
<rect width="{CANVAS_WIDTH}" height="{CANVAS_HEIGHT}" fill="url(#page-gradient)"/>
<circle cx="1400" cy="160" r="190" fill="#C46A2D" opacity="0.08"/>
<circle cx="124" cy="2160" r="180" fill="#1B7288" opacity="0.06"/>
<circle cx="1470" cy="2232" r="110" fill="#0E9683" opacity="0.08"/>
"##,
        escape_xml(&accessibility_desc)
    ));

    svg.push_str(&format!(
        r##"<text x="{PAGE_MARGIN}" y="132" class="hero-title">PubChem Molecular Topology</text>
"##
    ));
    svg.push_str(&text_block(
        PAGE_MARGIN,
        172,
        28,
        "hero-subtitle",
        &subtitle_lines,
    ));

    svg.push_str(&format!(
        r##"<g transform="translate({PAGE_MARGIN} {SUMMARY_Y})">
  <rect width="{CONTENT_WIDTH}" height="{SUMMARY_HEIGHT}" rx="30" fill="#FFFDF8" stroke="#E3D7C7" filter="url(#card-shadow)"/>
  <rect width="{CONTENT_WIDTH}" height="10" rx="30" fill="url(#hero-gradient)"/>
</g>
"##,
    ));
    let abstract_lines = wrap_text(&abstract_text, ABSTRACT_WIDTH as f32, ABSTRACT_FONT_SIZE);
    let abstract_x = PAGE_MARGIN + (CONTENT_WIDTH - ABSTRACT_WIDTH) / 2;
    let abstract_block_height = abstract_block_height(&abstract_lines);
    let abstract_y = SUMMARY_Y
        + (SUMMARY_HEIGHT.saturating_sub(abstract_block_height)) / 2
        + ABSTRACT_FONT_SIZE as usize;
    svg.push_str(&abstract_text_block(
        abstract_x,
        abstract_y,
        ABSTRACT_LINE_HEIGHT,
        "abstract",
        ABSTRACT_WIDTH,
        &abstract_lines,
    ));
    svg.push_str(&render_metric_histograms(report));
    svg.push_str(&render_implication_dag());

    for (index, spec) in CARD_SPECS.iter().enumerate() {
        let row = index / 2;
        let column = index % 2;
        let x = PAGE_MARGIN + column * (CARD_WIDTH + CARD_GAP_X);
        let y = CARD_START_Y + row * (CARD_HEIGHT + CARD_GAP_Y);
        let count = report.counts[spec.check as usize];
        svg.push_str(&render_card(
            spec,
            count,
            evaluated_records,
            max_count,
            x,
            y,
        ));
    }

    svg.push_str(&render_footer());
    svg.push_str("</svg>\n");
    Ok(svg)
}

fn render_card(
    spec: &CardSpec,
    count: u64,
    evaluated_records: u64,
    max_count: u64,
    x: usize,
    y: usize,
) -> String {
    let graphlet_scale = spec.graphlet_scale * GRAPHLET_SCALE_FACTOR;
    let share = if evaluated_records == 0 {
        0.0
    } else {
        100.0 * count as f64 / evaluated_records as f64
    };
    let bar_width = if max_count == 0 {
        56
    } else {
        (((count as f64 / max_count as f64) * CARD_BAR_WIDTH).round() as usize).max(24)
    };
    let bar_opacity = if count == 0 { "0.28" } else { "1.0" };
    let description_lines = wrap_text(spec.description, CARD_DESCRIPTION_WIDTH, 18.0);
    let graphlet = strip_root_svg(spec.graphlet);
    let graphlet_origin_x = GRAPHLET_FRAME_X as f32
        + (GRAPHLET_FRAME_SIZE as f32 - GRAPHLET_VIEWBOX_SIZE * graphlet_scale) / 2.0
        + spec.graphlet_shift_x as f32;
    let graphlet_origin_y = GRAPHLET_FRAME_Y as f32
        + (GRAPHLET_FRAME_SIZE as f32 - GRAPHLET_VIEWBOX_SIZE * graphlet_scale) / 2.0
        + spec.graphlet_shift_y as f32;

    format!(
        r##"<g transform="translate({x} {y})">
  <rect width="{CARD_WIDTH}" height="{CARD_HEIGHT}" rx="28" fill="#FFFDF8" stroke="#E3D7C7" filter="url(#card-shadow)"/>
  <rect width="{CARD_WIDTH}" height="12" rx="28" fill="{accent}"/>
  <rect x="{GRAPHLET_FRAME_X}" y="{GRAPHLET_FRAME_Y}" width="{GRAPHLET_FRAME_SIZE}" height="{GRAPHLET_FRAME_SIZE}" rx="22" fill="#F7F4EA" stroke="{accent}" stroke-opacity="0.16"/>
  <g transform="translate({graphlet_origin_x:.2} {graphlet_origin_y:.2}) scale({graphlet_scale})" color="{accent}" fill="none" style="--node-fill:#FFFDF8">
    {graphlet}
  </g>
  {title_markup}
  <text x="{CARD_TEXT_X}" y="126" class="card-count">{count}</text>
  <text x="{CARD_TEXT_X}" y="152" class="card-share">{share:.3}% of evaluated molecules</text>
  <rect x="{CARD_TEXT_X}" y="168" width="{CARD_BAR_WIDTH}" height="12" rx="6" fill="#E8E0D3"/>
  <rect x="{CARD_TEXT_X}" y="168" width="{bar_width}" height="12" rx="6" fill="{accent}" fill-opacity="{bar_opacity}"/>
  {description}
</g>
"##,
        accent = spec.accent,
        graphlet_origin_x = graphlet_origin_x,
        graphlet_origin_y = graphlet_origin_y,
        graphlet_scale = graphlet_scale,
        title_markup = render_card_label(
            CARD_TEXT_X,
            58,
            "start",
            "card-title",
            spec.title,
            "#102A43"
        ),
        count = escape_xml(&format_count(count)),
        description =
            text_block_with_graph_notation(CARD_TEXT_X, 210, 24, "card-body", &description_lines,),
    )
}

fn render_metric_histograms(report: &TopologyReport) -> String {
    let connected_components_bins =
        component_display_bins(&report.connected_components_histogram, 8);
    let diameter_bins = scalar_display_bins(&report.diameter_histogram, 8);
    let triangle_bins = scalar_display_bins(&report.triangle_count_histogram, 8);
    let square_bins = scalar_display_bins(&report.square_count_histogram, 8);
    let clustering_bins = coefficient_display_bins(&report.clustering_coefficient_histogram, 2);
    let square_clustering_bins =
        coefficient_display_bins(&report.square_clustering_coefficient_histogram, 2);

    format!(
        r##"<g transform="translate({PAGE_MARGIN} {HISTOGRAM_SECTION_Y})">
  {row_1_left}
  {row_1_right}
  {row_2_left}
  {row_2_right}
  {row_3_left}
  {row_3_right}
</g>
"##,
        row_1_left = render_histogram_panel(
            0,
            0,
            "Connected components",
            &connected_components_bins,
            "#1B7288",
            "Regenerate the dataset to populate this histogram.",
        ),
        row_1_right = render_histogram_panel(
            HISTOGRAM_PANEL_WIDTH + HISTOGRAM_PANEL_GAP,
            0,
            "Diameter",
            &diameter_bins,
            "#A5503A",
            "Defined for connected molecules only.",
        ),
        row_2_left = render_histogram_panel(
            0,
            HISTOGRAM_PANEL_HEIGHT + HISTOGRAM_ROW_GAP,
            "Triangle count",
            &triangle_bins,
            "#C46A2D",
            "Distinct 3-cycles per molecular graph.",
        ),
        row_2_right = render_histogram_panel(
            HISTOGRAM_PANEL_WIDTH + HISTOGRAM_PANEL_GAP,
            HISTOGRAM_PANEL_HEIGHT + HISTOGRAM_ROW_GAP,
            "Square count",
            &square_bins,
            "#7D3A1B",
            "Distinct 4-cycles per molecular graph.",
        ),
        row_3_left = render_histogram_panel(
            0,
            (HISTOGRAM_PANEL_HEIGHT + HISTOGRAM_ROW_GAP) * 2,
            "Clustering coefficient",
            &clustering_bins,
            "#2F6F65",
            "Mean local clustering over all graph nodes.",
        ),
        row_3_right = render_histogram_panel(
            HISTOGRAM_PANEL_WIDTH + HISTOGRAM_PANEL_GAP,
            (HISTOGRAM_PANEL_HEIGHT + HISTOGRAM_ROW_GAP) * 2,
            "Square clustering",
            &square_clustering_bins,
            "#3856A6",
            "Mean square clustering over all graph nodes.",
        ),
    )
}

fn render_implication_dag() -> String {
    let direct_edges = DAG_DIRECT_IMPLICATIONS
        .iter()
        .map(|(from, to)| {
            render_dag_direct_edge(
                dag_node_spec(*from),
                dag_node_spec(*to),
                "#52606D",
                "url(#dag-arrow)",
                3.0,
                0.94,
            )
        })
        .collect::<Vec<_>>()
        .join("\n");
    let exclusion_edges = DAG_EXCLUSIONS
        .iter()
        .map(|(from, to)| {
            render_dag_exclusion_edge(
                dag_node_spec(*from),
                dag_node_spec(*to),
                "#B13D44",
                "url(#dag-arrow-alert)",
                2.3,
                0.9,
            )
        })
        .collect::<Vec<_>>()
        .join("\n");
    let topology_nodes = DAG_TOPOLOGY_NODES
        .iter()
        .map(render_dag_node)
        .collect::<Vec<_>>()
        .join("\n");
    let obstruction_nodes = DAG_OBSTRUCTION_NODES
        .iter()
        .map(render_dag_node)
        .collect::<Vec<_>>()
        .join("\n");

    format!(
        r##"<g transform="translate({PAGE_MARGIN} {DAG_SECTION_Y})">
  <rect width="{CONTENT_WIDTH}" height="{DAG_PANEL_HEIGHT}" rx="28" fill="#FFFDF8" stroke="#E3D7C7" filter="url(#card-shadow)"/>
  <rect width="{CONTENT_WIDTH}" height="10" rx="28" fill="url(#hero-gradient)"/>
  <text x="32" y="40" class="panel-title">Implication map</text>
  <text x="32" y="62" class="panel-note">Read dark arrows as direct implications. Dashed red arrows mark forbidden subdivisions for the class above.</text>
  <g transform="translate({DAG_GRAPH_OFFSET_X} {DAG_GRAPH_OFFSET_Y})">
    <text x="1140" y="278" text-anchor="middle" class="panel-note">Forbidden subdivisions</text>
    {direct_edges}
    {exclusion_edges}
    {topology_nodes}
    {obstruction_nodes}
  </g>
  <g transform="translate({DAG_LEGEND_X} {DAG_LEGEND_Y})">
    <line x1="0" y1="0" x2="44" y2="0" stroke="#52606D" stroke-width="3.0" stroke-linecap="round" marker-end="url(#dag-arrow)"/>
    <text x="56" y="6" class="panel-note">direct</text>
    <line x1="176" y1="0" x2="220" y2="0" stroke="#B13D44" stroke-width="2.3" stroke-linecap="round" stroke-dasharray="10 8" marker-end="url(#dag-arrow-alert)"/>
    <text x="232" y="6" class="panel-note">forbids</text>
  </g>
</g>
"##,
    )
}

fn render_dag_node(node: &DagNodeSpec) -> String {
    let accent = card_spec_for(node.check).accent;
    let fill_opacity = if is_obstruction_check(node.check) {
        "0.10"
    } else {
        "0.08"
    };
    let stroke_opacity = if is_obstruction_check(node.check) {
        "0.44"
    } else {
        "0.36"
    };

    format!(
        r##"<g>
  <rect x="{x}" y="{y}" width="{width}" height="{height}" rx="18" fill="{accent}" fill-opacity="{fill_opacity}" stroke="{accent}" stroke-opacity="{stroke_opacity}" stroke-width="2"/>
  {label}
</g>"##,
        x = node.x,
        y = node.y,
        width = node.width,
        height = node.height,
        accent = accent,
        fill_opacity = fill_opacity,
        stroke_opacity = stroke_opacity,
        label = render_card_label(
            node.x + node.width / 2,
            node.y + node.height / 2 + 7,
            "middle",
            "dag-label",
            dag_node_label(node.check),
            accent,
        ),
    )
}

fn render_dag_direct_edge(
    from: &DagNodeSpec,
    to: &DagNodeSpec,
    stroke: &str,
    marker: &str,
    stroke_width: f64,
    opacity: f64,
) -> String {
    let from_center_x = (from.x + from.width / 2) as f64;
    let to_center_x = (to.x + to.width / 2) as f64;
    let path = if from.y == to.y {
        let y = (from.y + from.height / 2) as f64;
        let x1 = (from.x + from.width) as f64;
        let x2 = to.x as f64;
        format!("M {x1:.2} {y:.2} H {x2:.2}")
    } else if (from_center_x - to_center_x).abs() < 1.0 {
        if to.y < from.y {
            let y1 = from.y as f64;
            let y2 = (to.y + to.height) as f64;
            format!("M {from_center_x:.2} {y1:.2} V {y2:.2}")
        } else {
            let y1 = (from.y + from.height) as f64;
            let y2 = to.y as f64;
            format!("M {from_center_x:.2} {y1:.2} V {y2:.2}")
        }
    } else {
        let x1 = (from.x + from.width) as f64;
        let y1 = (from.y + from.height / 2) as f64;
        let x2 = to.x as f64;
        let y2 = (to.y + to.height / 2) as f64;
        let elbow_x = x1 + ((x2 - x1) * 0.44).clamp(40.0, 96.0);
        format!("M {x1:.2} {y1:.2} H {elbow_x:.2} V {y2:.2} H {x2:.2}")
    };

    format!(
        r##"<path d="{path}" fill="none" stroke="{stroke}" stroke-width="{stroke_width:.2}" stroke-opacity="{opacity:.2}" stroke-linecap="round" stroke-linejoin="round" marker-end="{marker}"/>"##,
    )
}

fn render_dag_exclusion_edge(
    from: &DagNodeSpec,
    to: &DagNodeSpec,
    stroke: &str,
    marker: &str,
    stroke_width: f64,
    opacity: f64,
) -> String {
    let x = (from.x + from.width / 2) as f64;
    let y1 = from.y as f64;
    let y2 = (to.y + to.height) as f64;

    format!(
        r##"<path d="M {x:.2} {y1:.2} V {y2:.2}" fill="none" stroke="{stroke}" stroke-width="{stroke_width:.2}" stroke-opacity="{opacity:.2}" stroke-linecap="round" stroke-linejoin="round" stroke-dasharray="10 8" marker-end="{marker}"/>"##,
    )
}

fn dag_node_label(check: Check) -> &'static str {
    match check {
        Check::K23Homeomorph => "K2,3",
        Check::K33Homeomorph => "K3,3",
        Check::K4Homeomorph => "K4",
        _ => card_spec_for(check).title,
    }
}

fn card_spec_for(check: Check) -> &'static CardSpec {
    match check {
        Check::Tree => &CARD_SPECS[0],
        Check::Forest => &CARD_SPECS[1],
        Check::Cactus => &CARD_SPECS[2],
        Check::Chordal => &CARD_SPECS[3],
        Check::Planar => &CARD_SPECS[4],
        Check::Outerplanar => &CARD_SPECS[5],
        Check::K23Homeomorph => &CARD_SPECS[6],
        Check::K33Homeomorph => &CARD_SPECS[7],
        Check::K4Homeomorph => &CARD_SPECS[8],
        Check::Bipartite => &CARD_SPECS[9],
    }
}

fn dag_node_spec(check: Check) -> &'static DagNodeSpec {
    match check {
        Check::Tree => &DAG_TOPOLOGY_NODES[0],
        Check::Forest => &DAG_TOPOLOGY_NODES[1],
        Check::Cactus => &DAG_TOPOLOGY_NODES[2],
        Check::Chordal => &DAG_TOPOLOGY_NODES[3],
        Check::Bipartite => &DAG_TOPOLOGY_NODES[4],
        Check::Outerplanar => &DAG_TOPOLOGY_NODES[5],
        Check::Planar => &DAG_TOPOLOGY_NODES[6],
        Check::K23Homeomorph => &DAG_OBSTRUCTION_NODES[0],
        Check::K4Homeomorph => &DAG_OBSTRUCTION_NODES[1],
        Check::K33Homeomorph => &DAG_OBSTRUCTION_NODES[2],
    }
}

fn is_obstruction_check(check: Check) -> bool {
    matches!(
        check,
        Check::K23Homeomorph | Check::K33Homeomorph | Check::K4Homeomorph
    )
}

fn render_histogram_panel(
    x: usize,
    y: usize,
    title: &str,
    display_bins: &[(String, u64)],
    accent: &str,
    empty_note: &str,
) -> String {
    let max_molecules = display_bins
        .iter()
        .map(|(_, molecules)| *molecules)
        .max()
        .unwrap_or(0);
    let chart_x = 34.0_f64;
    let chart_y = 58.0_f64;
    let chart_width = 652.0_f64;
    let chart_height = 54.0_f64;
    let baseline_y = chart_y + chart_height;
    let slot_width = if display_bins.is_empty() {
        chart_width
    } else {
        chart_width / display_bins.len() as f64
    };

    let bars = if display_bins.is_empty() {
        format!(
            r##"<text x="{}" y="{}" class="panel-note">{}</text>"##,
            chart_x,
            chart_y + 24.0,
            escape_xml(empty_note),
        )
    } else {
        display_bins
            .iter()
            .enumerate()
            .map(|(index, (label, molecules))| {
                let bar_width = (slot_width * 0.58).max(18.0);
                let bar_x = chart_x + index as f64 * slot_width + (slot_width - bar_width) / 2.0;
                let bar_height = if max_molecules == 0 {
                    0.0
                } else {
                    ((*molecules as f64 / max_molecules as f64) * chart_height).max(4.0)
                };
                let bar_y = baseline_y - bar_height;
                format!(
                    r##"<rect x="{bar_x:.2}" y="{bar_y:.2}" width="{bar_width:.2}" height="{bar_height:.2}" rx="8" fill="{accent}" fill-opacity="0.86"/>
<text x="{label_x:.2}" y="{label_y:.2}" text-anchor="middle" class="hist-label">{label}</text>
<text x="{label_x:.2}" y="{value_y:.2}" text-anchor="middle" class="hist-value">{value}</text>"##,
                    label_x = bar_x + bar_width / 2.0,
                    label_y = baseline_y + 22.0,
                    value_y = bar_y - 8.0,
                    value = escape_xml(&format_compact_count(*molecules)),
                )
            })
            .collect::<Vec<_>>()
            .join("\n")
    };

    format!(
        r##"<g transform="translate({x} {y})">
  <rect width="{HISTOGRAM_PANEL_WIDTH}" height="{HISTOGRAM_PANEL_HEIGHT}" rx="28" fill="#FFFDF8" stroke="#E3D7C7" filter="url(#card-shadow)"/>
  <rect width="{HISTOGRAM_PANEL_WIDTH}" height="10" rx="28" fill="{accent}" fill-opacity="0.92"/>
  <text x="32" y="34" class="panel-title">{title}</text>
  <line x1="{chart_x}" y1="{baseline_y}" x2="{chart_x_end}" y2="{baseline_y}" stroke="#D9E2EC" stroke-width="2"/>
  {bars}
</g>"##,
        chart_x_end = chart_x + chart_width,
        title = escape_xml(title),
        bars = bars,
    )
}

fn coefficient_display_bins(
    histogram: &[CoefficientHistogramBin],
    bins_per_display_bucket: usize,
) -> Vec<(String, u64)> {
    histogram
        .chunks(bins_per_display_bucket.max(1))
        .map(|chunk| {
            let lower_bound = chunk.first().map_or(0.0, |bin| bin.lower_bound);
            let upper_bound = chunk.last().map_or(1.0, |bin| bin.upper_bound);
            let molecules = chunk.iter().map(|bin| bin.molecules).sum();
            (format!("{lower_bound:.1}-{upper_bound:.1}"), molecules)
        })
        .collect()
}

fn render_footer() -> String {
    let commit = current_commit_short().unwrap_or_else(|| "unknown".to_owned());
    let generated_on = OffsetDateTime::now_utc().date().to_string();
    let footer_text = format!("{REPOSITORY_URL}  •  commit {commit}  •  {generated_on}");

    format!(
        r##"<g transform="translate({PAGE_MARGIN} {FOOTER_Y})">
  <line x1="0" y1="0" x2="{CONTENT_WIDTH}" y2="0" stroke="#D9E2EC" stroke-width="2"/>
  <text x="{}" y="28" text-anchor="middle" class="muted">{}</text>
</g>
"##,
        CONTENT_WIDTH / 2,
        escape_xml(&footer_text),
    )
}

fn text_block(x: usize, y: usize, line_height: usize, class: &str, lines: &[String]) -> String {
    lines
        .iter()
        .enumerate()
        .map(|(index, line)| {
            format!(
                r##"<text x="{x}" y="{}" class="{class}">{}</text>"##,
                y + index * line_height,
                escape_xml(line),
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn text_block_with_graph_notation(
    x: usize,
    y: usize,
    line_height: usize,
    class: &str,
    lines: &[String],
) -> String {
    lines
        .iter()
        .enumerate()
        .map(|(index, line)| {
            format!(
                r##"<text x="{x}" y="{}" class="{class}">{}</text>"##,
                y + index * line_height,
                format_inline_graph_notation(line),
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn render_card_label(
    x: usize,
    y: usize,
    anchor: &str,
    class: &str,
    text: &str,
    fill: &str,
) -> String {
    let styled = match text {
        "K2,3" => format_k_notation("2", Some("3"), None),
        "K3,3" => format_k_notation("3", Some("3"), None),
        "K4" => format_k_notation("4", None, None),
        "K2,3 Homeomorph" => format_k_notation("2", Some("3"), Some(" Homeomorph")),
        "K3,3 Homeomorph" => format_k_notation("3", Some("3"), Some(" Homeomorph")),
        "K4 Homeomorph" => format_k_notation("4", None, Some(" Homeomorph")),
        _ => escape_xml(text),
    };

    format!(
        r##"<text x="{x}" y="{y}" text-anchor="{anchor}" class="{class}" fill="{fill}">{styled}</text>"##
    )
}

fn abstract_text_block(
    x: usize,
    y: usize,
    line_height: usize,
    class: &str,
    width: usize,
    lines: &[String],
) -> String {
    let mut used_bold_phrase = false;
    let width_f32 = width as f32;
    lines
        .iter()
        .enumerate()
        .map(|(index, line)| {
            let markup = if !used_bold_phrase && line.contains("PubChem Molecular Topology") {
                used_bold_phrase = true;
                format_abstract_line(line)
            } else {
                format_inline_graph_notation(line)
            };
            let justify =
                index + 1 < lines.len() && estimated_text_width(line, 18.0) > width_f32 * 0.72;
            let justification_attributes = if justify {
                format!(r#" textLength="{width}" lengthAdjust="spacing""#)
            } else {
                String::new()
            };

            format!(
                r##"<text x="{x}" y="{}" class="{class}"{}>{markup}</text>"##,
                y + index * line_height,
                justification_attributes,
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn abstract_block_height(lines: &[String]) -> usize {
    if lines.is_empty() {
        return 0;
    }

    (lines.len() - 1) * ABSTRACT_LINE_HEIGHT + ABSTRACT_FONT_SIZE.ceil() as usize
}

fn format_abstract_line(line: &str) -> String {
    const TITLE_PHRASE: &str = "PubChem Molecular Topology";

    if let Some((before, after)) = line.split_once(TITLE_PHRASE) {
        return format!(
            "{}<tspan font-weight=\"760\" fill=\"#243B53\">{}</tspan>{}",
            format_inline_graph_notation(before),
            escape_xml(TITLE_PHRASE),
            format_inline_graph_notation(after),
        );
    }

    format_inline_graph_notation(line)
}

fn format_k_notation(
    left_subscript: &str,
    right_subscript: Option<&str>,
    suffix: Option<&str>,
) -> String {
    let mut output = String::from("<tspan>K</tspan>");
    output.push_str(&format!(
        r##"<tspan baseline-shift="-18%" font-size="72%">{}</tspan>"##,
        escape_xml(left_subscript),
    ));

    if let Some(right_subscript) = right_subscript {
        output.push_str(r##"<tspan baseline-shift="-18%" font-size="48%">,</tspan>"##);
        output.push_str(&format!(
            r##"<tspan baseline-shift="-18%" font-size="72%">{}</tspan>"##,
            escape_xml(right_subscript),
        ));
    }

    if let Some(suffix) = suffix {
        output.push_str(&format!(r##"<tspan>{}</tspan>"##, escape_xml(suffix)));
    }

    output
}

fn format_inline_graph_notation(text: &str) -> String {
    let mut formatted = escape_xml(text);
    formatted = formatted.replace("K2,3", &format_k_notation("2", Some("3"), None));
    formatted = formatted.replace("K3,3", &format_k_notation("3", Some("3"), None));
    formatted = formatted.replace("K4", &format_k_notation("4", None, None));
    formatted
}

fn current_commit_short() -> Option<String> {
    let output = Command::new("git")
        .args(["rev-parse", "--short=12", "HEAD"])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .ok()?;

    if !output.status.success() {
        return None;
    }

    let commit = String::from_utf8(output.stdout).ok()?;
    let trimmed = commit.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_owned())
    }
}

fn wrap_text(text: &str, max_width: f32, font_size: f32) -> Vec<String> {
    let mut lines = Vec::new();
    let mut current = String::new();

    for word in text.split_whitespace() {
        let candidate = if current.is_empty() {
            word.to_owned()
        } else {
            format!("{current} {word}")
        };

        if !current.is_empty() && estimated_text_width(&candidate, font_size) > max_width {
            lines.push(current);
            current = word.to_owned();
        } else {
            current = candidate;
        }
    }

    if !current.is_empty() {
        lines.push(current);
    }

    lines
}

fn estimated_text_width(text: &str, font_size: f32) -> f32 {
    text.chars()
        .map(|ch| font_size * glyph_width_factor(ch))
        .sum()
}

fn glyph_width_factor(ch: char) -> f32 {
    match ch {
        'i' | 'l' | '!' | '.' | ',' | ':' | ';' | '\'' | '"' | '|' => 0.28,
        'f' | 'j' | 'r' | 't' | '(' | ')' | '[' | ']' => 0.36,
        ' ' => 0.30,
        'm' | 'w' | 'M' | 'W' | '@' | '#' | '%' | '&' => 0.82,
        'A'..='Z' => 0.64,
        '0'..='9' => 0.58,
        _ => 0.54,
    }
}

fn format_count(value: u64) -> String {
    let digits = value.to_string();
    let mut reversed = String::new();
    for (index, ch) in digits.chars().rev().enumerate() {
        if index > 0 && index % 3 == 0 {
            reversed.push(',');
        }
        reversed.push(ch);
    }
    reversed.chars().rev().collect()
}

fn format_compact_count(value: u64) -> String {
    if value >= 1_000_000 {
        return format!("{:.1}M", value as f64 / 1_000_000.0);
    }
    if value >= 1_000 {
        return format!("{:.1}k", value as f64 / 1_000.0);
    }
    format_count(value)
}

fn component_display_bins(
    histogram: &[ComponentHistogramBin],
    max_bins: usize,
) -> Vec<(String, u64)> {
    if histogram.len() <= max_bins {
        return histogram
            .iter()
            .map(|bin| (bin.component_count.to_string(), bin.molecules))
            .collect();
    }

    let keep = max_bins.saturating_sub(1);
    let mut bins = histogram
        .iter()
        .take(keep)
        .map(|bin| (bin.component_count.to_string(), bin.molecules))
        .collect::<Vec<_>>();
    let overflow_label = format!("{}+", histogram[keep].component_count);
    let overflow_value = histogram.iter().skip(keep).map(|bin| bin.molecules).sum();
    bins.push((overflow_label, overflow_value));
    bins
}

fn scalar_display_bins(histogram: &[ScalarHistogramBin], max_bins: usize) -> Vec<(String, u64)> {
    if histogram.len() <= max_bins {
        return histogram
            .iter()
            .map(|bin| (bin.value.to_string(), bin.molecules))
            .collect();
    }

    let keep = max_bins.saturating_sub(1);
    let mut bins = histogram
        .iter()
        .take(keep)
        .map(|bin| (bin.value.to_string(), bin.molecules))
        .collect::<Vec<_>>();
    let overflow_label = format!("{}+", histogram[keep].value);
    let overflow_value = histogram.iter().skip(keep).map(|bin| bin.molecules).sum();
    bins.push((overflow_label, overflow_value));
    bins
}

fn build_abstract_text(report: &TopologyReport, evaluated_records: u64) -> String {
    let runtime_seconds = format_elapsed_seconds_decimal(report.pipeline_elapsed_seconds);
    let planar_count = format_count(report.counts[Check::Planar as usize]);
    let outerplanar_count = format_count(report.counts[Check::Outerplanar as usize]);
    let cactus_count = format_count(report.counts[Check::Cactus as usize]);
    let bipartite_count = format_count(report.counts[Check::Bipartite as usize]);
    let k23_count = format_count(report.counts[Check::K23Homeomorph as usize]);
    let k33_count = format_count(report.counts[Check::K33Homeomorph as usize]);
    let k4_count = format_count(report.counts[Check::K4Homeomorph as usize]);

    if report.parse_errors == 0 && report.topology_errors == 0 {
        return format!(
            "PubChem Molecular Topology provides a reproducible workflow for graph-theoretic annotation of molecular structures in PubChem. In the current CID-SMILES release, {} compounds were downloaded, decompressed, parsed, and evaluated in {} with no parsing or topology failures. The release contained {} planar graphs, {} outerplanar graphs, {} cactus graphs, {} bipartite graphs, {} K2,3 homeomorphs, {} K4 homeomorphs, and {} K3,3 homeomorphs. Per-CID boolean annotations were written to Parquet, and connected-component counts plus exact diameters for connected molecules were stored alongside them, where the diameter is the longest of the shortest paths, while aggregate summaries were accumulated in the same pass. Future work can test whether these topologies and motif-level metrics, including triangle counts and square clustering, correlate with interesting chemical properties. The source code is available on GitHub, and released result sets are deposited in Zenodo.",
            format_count(report.total_records),
            runtime_seconds,
            planar_count,
            outerplanar_count,
            cactus_count,
            bipartite_count,
            k23_count,
            k4_count,
            k33_count,
        );
    }

    format!(
        "PubChem Molecular Topology provides a reproducible workflow for graph-theoretic annotation of molecular structures in PubChem. In the current CID-SMILES release, {} records were downloaded and decompressed; {} were parsed into molecular graphs, {} failed parsing, and {} completed topology evaluation after {} topology failures in {}. Among the evaluated graphs, {} were planar, {} were outerplanar, {} were cactus graphs, {} were bipartite, {} contained K2,3 homeomorphs, {} contained K4 homeomorphs, and {} contained K3,3 homeomorphs. Per-CID boolean annotations were written to Parquet, and connected-component counts plus exact diameters for connected molecules were stored alongside them, where the diameter is the longest of the shortest paths, while aggregate summaries were accumulated in the same pass. Future work can test whether these topologies and motif-level metrics, including triangle counts and square clustering, correlate with interesting chemical properties. The source code is available on GitHub, and released result sets are deposited in Zenodo.",
        format_count(report.total_records),
        format_count(report.parsed_records),
        format_count(report.parse_errors),
        format_count(evaluated_records),
        format_count(report.topology_errors),
        runtime_seconds,
        planar_count,
        outerplanar_count,
        cactus_count,
        bipartite_count,
        k23_count,
        k4_count,
        k33_count,
    )
}

fn format_elapsed_seconds_decimal(seconds: f64) -> String {
    if seconds < 0.05 {
        return "<0.1 s".to_owned();
    }

    format!("{seconds:.1} s")
}

fn escape_xml(text: &str) -> String {
    let mut escaped = String::with_capacity(text.len());
    for ch in text.chars() {
        match ch {
            '&' => escaped.push_str("&amp;"),
            '<' => escaped.push_str("&lt;"),
            '>' => escaped.push_str("&gt;"),
            '"' => escaped.push_str("&quot;"),
            '\'' => escaped.push_str("&apos;"),
            _ => escaped.push(ch),
        }
    }
    escaped
}

fn strip_root_svg(svg: &str) -> String {
    let inner = if let Some((_, tail)) = svg.split_once('>') {
        if let Some((content, _)) = tail.rsplit_once("</svg>") {
            content
        } else {
            svg
        }
    } else {
        svg
    };

    strip_title(inner).trim().to_owned()
}

fn strip_title(svg: &str) -> String {
    let mut remaining = svg;
    let mut output = String::new();

    loop {
        let Some(start) = remaining.find("<title>") else {
            output.push_str(remaining);
            break;
        };
        output.push_str(&remaining[..start]);
        let after_start = &remaining[start + "<title>".len()..];
        let Some(end) = after_start.find("</title>") else {
            break;
        };
        remaining = &after_start[end + "</title>".len()..];
    }

    output
}

#[cfg(test)]
mod tests {
    use super::{
        CARD_DESCRIPTION_WIDTH, CARD_SPECS, format_count, infographic_svg, strip_root_svg,
        wrap_text, write_infographic,
    };
    use crate::{
        Check, CoefficientHistogramBin, ComponentHistogramBin, ScalarHistogramBin, TopologyReport,
    };
    use anyhow::Result;
    use std::{fs, path::PathBuf};
    use tempfile::tempdir;
    use topology_classifier::graphlet_svg;

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

    fn zero_failure_report() -> TopologyReport {
        TopologyReport {
            parse_errors: 0,
            parsed_records: 4,
            topology_errors: 0,
            counts: [1, 1, 4, 1, 4, 4, 0, 0, 0, 4],
            connected_components_histogram: vec![
                ComponentHistogramBin {
                    component_count: 1,
                    molecules: 3,
                },
                ComponentHistogramBin {
                    component_count: 2,
                    molecules: 1,
                },
            ],
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
                    molecules: 2,
                },
            ],
            square_count_histogram: vec![
                ScalarHistogramBin {
                    value: 0,
                    molecules: 3,
                },
                ScalarHistogramBin {
                    value: 1,
                    molecules: 1,
                },
            ],
            clustering_coefficient_histogram: vec![
                CoefficientHistogramBin {
                    lower_bound: 0.0,
                    upper_bound: 0.1,
                    molecules: 2,
                },
                CoefficientHistogramBin {
                    lower_bound: 0.4,
                    upper_bound: 0.5,
                    molecules: 1,
                },
                CoefficientHistogramBin {
                    lower_bound: 0.9,
                    upper_bound: 1.0,
                    molecules: 1,
                },
            ],
            square_clustering_coefficient_histogram: vec![
                CoefficientHistogramBin {
                    lower_bound: 0.0,
                    upper_bound: 0.1,
                    molecules: 3,
                },
                CoefficientHistogramBin {
                    lower_bound: 0.9,
                    upper_bound: 1.0,
                    molecules: 1,
                },
            ],
            ..sample_report()
        }
    }

    #[test]
    fn format_count_adds_thousands_separators() {
        assert_eq!(format_count(120_000_000), "120,000,000");
    }

    #[test]
    fn wrap_text_preserves_words_without_dropping_content() {
        let lines = wrap_text("alpha beta gamma delta epsilon", 96.0, 12.0);
        assert_eq!(lines, vec!["alpha beta", "gamma delta", "epsilon"]);
    }

    #[test]
    fn card_descriptions_fit_within_two_lines() {
        for spec in CARD_SPECS {
            let lines = wrap_text(spec.description, CARD_DESCRIPTION_WIDTH, 18.0);
            assert!(
                lines.len() <= 2,
                "{} wrapped to {} lines: {:?}",
                spec.title,
                lines.len(),
                lines
            );
        }
    }

    #[test]
    fn strip_root_svg_keeps_graphlet_contents_only() {
        let inner = strip_root_svg(graphlet_svg(Check::Bipartite));
        assert!(inner.contains("<circle"));
        assert!(!inner.contains("<svg"));
        assert!(!inner.contains("<title>"));
    }

    #[test]
    fn infographic_svg_contains_key_sections() -> Result<()> {
        let svg = infographic_svg(&sample_report())?;
        assert!(svg.contains("PubChem Molecular Topology"));
        assert!(svg.contains("font-weight=\"760\""));
        assert!(svg.contains("Zenodo"));
        assert!(svg.contains("Homeomorph"));
        assert!(svg.contains("Connected components"));
        assert!(svg.contains("Diameter"));
        assert!(svg.contains("Implication map"));
        assert!(svg.contains("Forbidden subdivisions"));
        assert!(svg.contains("direct"));
        assert!(svg.contains(env!("CARGO_PKG_REPOSITORY")));
        assert!(svg.contains("in 97.0 s."));
        assert!(svg.contains(r#"baseline-shift="-18%""#));
        assert!(!svg.contains("<foreignObject"));
        Ok(())
    }

    #[test]
    fn infographic_hides_redundant_success_tiles_when_no_failures_occur() -> Result<()> {
        let svg = infographic_svg(&zero_failure_report())?;
        assert!(!svg.contains("Total records"));
        assert!(!svg.contains("SMILES parsed"));
        assert!(!svg.contains("Topology evaluated"));
        assert!(!svg.contains("Analysis runtime"));
        assert!(svg.contains("with no parsing or topology failures"));
        assert!(svg.contains("were downloaded, decompressed, parsed, and evaluated in "));
        assert!(!svg.contains("Identical in this run."));
        Ok(())
    }

    #[test]
    fn write_infographic_writes_svg_file() -> Result<()> {
        let tempdir = tempdir()?;
        let path = tempdir.path().join("infographic.svg");
        write_infographic(&sample_report(), &path)?;
        let svg = fs::read_to_string(path)?;
        assert!(svg.contains("PubChem Molecular Topology"));
        assert!(svg.contains("Tree"));
        assert!(svg.contains("Bipartite"));
        Ok(())
    }
}
