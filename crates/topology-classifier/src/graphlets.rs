//! Shared graphlet SVGs used by the infographic and browser app.

use crate::Check;

/// Returns the canonical SVG graphlet for a topology predicate.
pub const fn graphlet_svg(check: Check) -> &'static str {
    match check {
        Check::Tree => TREE_GRAPHLET,
        Check::Forest => FOREST_GRAPHLET,
        Check::Cactus => CACTUS_GRAPHLET,
        Check::Chordal => CHORDAL_GRAPHLET,
        Check::Planar => PLANAR_GRAPHLET,
        Check::Outerplanar => OUTERPLANAR_GRAPHLET,
        Check::K23Homeomorph => K23_GRAPHLET,
        Check::K33Homeomorph => K33_GRAPHLET,
        Check::K4Homeomorph => K4_GRAPHLET,
        Check::Bipartite => BIPARTITE_GRAPHLET,
    }
}

const TREE_GRAPHLET: &str = r###"<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 160 160" fill="none" role="img" aria-label="Tree graphlet">
  <g stroke="currentColor" stroke-width="4" stroke-linecap="round" stroke-linejoin="round">
    <line x1="80" y1="128" x2="56" y2="96"/>
    <line x1="80" y1="128" x2="104" y2="96"/>
    <line x1="56" y1="96" x2="32" y2="56"/>
    <line x1="56" y1="96" x2="64" y2="56"/>
    <line x1="104" y1="96" x2="96" y2="56"/>
    <line x1="104" y1="96" x2="128" y2="56"/>
  </g>
  <g stroke="currentColor" stroke-width="3">
    <circle cx="80" cy="128" r="12" fill="var(--node-fill, #F7F4EA)"/>
    <circle cx="56" cy="96" r="9" fill="var(--node-fill, #F7F4EA)"/>
    <circle cx="104" cy="96" r="9" fill="var(--node-fill, #F7F4EA)"/>
    <circle cx="32" cy="56" r="8" fill="var(--node-fill, #F7F4EA)"/>
    <circle cx="64" cy="56" r="8" fill="var(--node-fill, #F7F4EA)"/>
    <circle cx="96" cy="56" r="8" fill="var(--node-fill, #F7F4EA)"/>
    <circle cx="128" cy="56" r="8" fill="var(--node-fill, #F7F4EA)"/>
  </g>
</svg>
"###;

const FOREST_GRAPHLET: &str = r###"<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 160 160" fill="none" role="img" aria-label="Forest graphlet">
  <g stroke="currentColor" stroke-width="3" stroke-linecap="round" stroke-linejoin="round">
    <path d="M36 62 L24 42"/>
    <path d="M36 62 L48 42"/>
    <path d="M36 62 L36 88"/>
    <circle cx="36" cy="62" r="6" fill="var(--node-fill, #F7F4EA)" stroke="currentColor"/>
    <circle cx="24" cy="42" r="6" fill="var(--node-fill, #F7F4EA)" stroke="currentColor"/>
    <circle cx="48" cy="42" r="6" fill="var(--node-fill, #F7F4EA)" stroke="currentColor"/>
    <circle cx="36" cy="88" r="6" fill="var(--node-fill, #F7F4EA)" stroke="currentColor"/>

    <path d="M80 46 L68 62"/>
    <path d="M80 46 L92 62"/>
    <path d="M80 46 L80 78"/>
    <path d="M80 78 L72 98"/>
    <path d="M80 78 L88 98"/>
    <circle cx="80" cy="46" r="6" fill="var(--node-fill, #F7F4EA)" stroke="currentColor"/>
    <circle cx="68" cy="62" r="6" fill="var(--node-fill, #F7F4EA)" stroke="currentColor"/>
    <circle cx="92" cy="62" r="6" fill="var(--node-fill, #F7F4EA)" stroke="currentColor"/>
    <circle cx="80" cy="78" r="6" fill="var(--node-fill, #F7F4EA)" stroke="currentColor"/>
    <circle cx="72" cy="98" r="6" fill="var(--node-fill, #F7F4EA)" stroke="currentColor"/>
    <circle cx="88" cy="98" r="6" fill="var(--node-fill, #F7F4EA)" stroke="currentColor"/>

    <path d="M124 62 L112 42"/>
    <path d="M124 62 L136 42"/>
    <path d="M124 62 L124 88"/>
    <circle cx="124" cy="62" r="6" fill="var(--node-fill, #F7F4EA)" stroke="currentColor"/>
    <circle cx="112" cy="42" r="6" fill="var(--node-fill, #F7F4EA)" stroke="currentColor"/>
    <circle cx="136" cy="42" r="6" fill="var(--node-fill, #F7F4EA)" stroke="currentColor"/>
    <circle cx="124" cy="88" r="6" fill="var(--node-fill, #F7F4EA)" stroke="currentColor"/>
  </g>
</svg>
"###;

const CACTUS_GRAPHLET: &str = r###"<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 160 160" fill="none" aria-hidden="true">
  <g stroke="currentColor" stroke-width="3.5" stroke-linecap="round" stroke-linejoin="round">
    <path d="M80 80 L44 54 L44 106 Z"/>
    <path d="M80 80 L116 54 L116 106 Z"/>
    <path d="M80 80 L80 132"/>
  </g>

  <g stroke="currentColor" stroke-width="2.2">
    <circle cx="80" cy="80" r="6.5" fill="var(--node-fill, #F7F4EA)"/>
    <circle cx="44" cy="54" r="5.5" fill="var(--node-fill, #F7F4EA)"/>
    <circle cx="44" cy="106" r="5.5" fill="var(--node-fill, #F7F4EA)"/>
    <circle cx="116" cy="54" r="5.5" fill="var(--node-fill, #F7F4EA)"/>
    <circle cx="116" cy="106" r="5.5" fill="var(--node-fill, #F7F4EA)"/>
    <circle cx="80" cy="132" r="5.5" fill="var(--node-fill, #F7F4EA)"/>
  </g>
</svg>
"###;

const CHORDAL_GRAPHLET: &str = r###"<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 160 160" fill="none" aria-hidden="true">
  <g stroke="currentColor" stroke-width="3.8" stroke-linecap="round" stroke-linejoin="round">
    <g opacity="0.62">
      <path d="M36 36H124"/>
      <path d="M124 36V124"/>
      <path d="M124 124H36"/>
      <path d="M36 124V36"/>
    </g>
    <path d="M36 124L124 36"/>
  </g>
  <g fill="var(--node-fill, #F7F4EA)" stroke="currentColor" stroke-width="2.8">
    <circle cx="36" cy="36" r="8"/>
    <circle cx="124" cy="36" r="8"/>
    <circle cx="124" cy="124" r="8"/>
    <circle cx="36" cy="124" r="8"/>
  </g>
</svg>
"###;

const PLANAR_GRAPHLET: &str = r###"<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 160 160" role="img" aria-label="Planar graphlet">
  <g fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round">
    <path d="M80 30 L123.3 55 L123.3 105 L80 130 L36.7 105 L36.7 55 Z"/>
    <path d="M80 80 L80 30"/>
    <path d="M80 80 L123.3 55"/>
    <path d="M80 80 L123.3 105"/>
    <path d="M80 80 L80 130"/>
    <path d="M80 80 L36.7 105"/>
    <path d="M80 80 L36.7 55"/>
  </g>

  <g stroke="currentColor" stroke-width="2.5">
    <circle cx="80" cy="30" r="6.5" fill="var(--node-fill, #F7F4EA)"/>
    <circle cx="123.3" cy="55" r="6.5" fill="var(--node-fill, #F7F4EA)"/>
    <circle cx="123.3" cy="105" r="6.5" fill="var(--node-fill, #F7F4EA)"/>
    <circle cx="80" cy="130" r="6.5" fill="var(--node-fill, #F7F4EA)"/>
    <circle cx="36.7" cy="105" r="6.5" fill="var(--node-fill, #F7F4EA)"/>
    <circle cx="36.7" cy="55" r="6.5" fill="var(--node-fill, #F7F4EA)"/>
    <circle cx="80" cy="80" r="7.5" fill="var(--node-fill, #F7F4EA)"/>
  </g>
</svg>
"###;

const OUTERPLANAR_GRAPHLET: &str = r###"<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 160 160" fill="none" stroke-linecap="round" stroke-linejoin="round" style="color:#364152">
  <g stroke="currentColor" stroke-width="4">
    <line x1="80" y1="24" x2="126" y2="50"/>
    <line x1="126" y1="50" x2="126" y2="110"/>
    <line x1="126" y1="110" x2="80" y2="136"/>
    <line x1="80" y1="136" x2="34" y2="110"/>
    <line x1="34" y1="110" x2="34" y2="50"/>
    <line x1="34" y1="50" x2="80" y2="24"/>
    <line x1="80" y1="24" x2="126" y2="110"/>
    <line x1="126" y1="110" x2="34" y2="110"/>
    <line x1="34" y1="110" x2="80" y2="24"/>
  </g>
  <g stroke="currentColor" stroke-width="2">
    <circle cx="80" cy="24" r="7" fill="var(--node-fill, #F7F4EA)"/>
    <circle cx="126" cy="50" r="7" fill="var(--node-fill, #F7F4EA)"/>
    <circle cx="126" cy="110" r="7" fill="var(--node-fill, #F7F4EA)"/>
    <circle cx="80" cy="136" r="7" fill="var(--node-fill, #F7F4EA)"/>
    <circle cx="34" cy="110" r="7" fill="var(--node-fill, #F7F4EA)"/>
    <circle cx="34" cy="50" r="7" fill="var(--node-fill, #F7F4EA)"/>
  </g>
</svg>
"###;

const K23_GRAPHLET: &str = r###"<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 160 160" role="img" aria-labelledby="title desc" fill="none">
  <title id="title">K2,3 homeomorph</title>
  <desc id="desc">The complete bipartite graph K2,3 drawn with two vertices in one partition and three in the other.</desc>
  <g stroke="currentColor" stroke-width="2.8" stroke-linecap="round" stroke-linejoin="round" opacity="0.94">
    <path d="M44 34L18 124"/>
    <path d="M44 34L80 124"/>
    <path d="M44 34L142 124"/>

    <path d="M116 34L18 124"/>
    <path d="M116 34L80 124"/>
    <path d="M116 34L142 124"/>
  </g>

  <g fill="var(--node-fill, #F7F4EA)" stroke="currentColor" stroke-width="2.8">
    <circle cx="44" cy="34" r="7.8"/>
    <circle cx="116" cy="34" r="7.8"/>

    <circle cx="18" cy="124" r="7.8"/>
    <circle cx="80" cy="124" r="7.8"/>
    <circle cx="142" cy="124" r="7.8"/>
  </g>
</svg>
"###;

const K33_GRAPHLET: &str = r###"<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 160 160" fill="none" role="img" aria-label="K3,3 homeomorph graphlet">
  <g stroke="currentColor" stroke-width="2.8" stroke-linecap="round" stroke-linejoin="round" opacity="0.94">
    <path d="M28 34L28 126"/>
    <path d="M28 34L80 126"/>
    <path d="M28 34L132 126"/>

    <path d="M80 34L28 126"/>
    <path d="M80 34L80 126"/>
    <path d="M80 34L132 126"/>

    <path d="M132 34L28 126"/>
    <path d="M132 34L80 126"/>
    <path d="M132 34L132 126"/>
  </g>

  <g fill="var(--node-fill, #F7F4EA)" stroke="currentColor" stroke-width="2.8">
    <circle cx="28" cy="34" r="7.6"/>
    <circle cx="80" cy="34" r="7.6"/>
    <circle cx="132" cy="34" r="7.6"/>

    <circle cx="28" cy="126" r="7.6"/>
    <circle cx="80" cy="126" r="7.6"/>
    <circle cx="132" cy="126" r="7.6"/>
  </g>
</svg>
"###;

const K4_GRAPHLET: &str = r###"<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 160 160" fill="none" role="img" aria-label="K4 homeomorph graphlet">
  <g stroke="currentColor" stroke-width="3.6" stroke-linecap="round" stroke-linejoin="round" opacity="0.94">
    <path d="M80 24L34 118"/>
    <path d="M80 24L126 118"/>
    <path d="M34 118L126 118"/>

    <path d="M80 72L80 24"/>
    <path d="M80 72L34 118"/>
    <path d="M80 72L126 118"/>
  </g>

  <g fill="var(--node-fill, #F7F4EA)" stroke="currentColor" stroke-width="2.8">
    <circle cx="80" cy="24" r="8.2"/>
    <circle cx="34" cy="118" r="8.2"/>
    <circle cx="126" cy="118" r="8.2"/>
    <circle cx="80" cy="72" r="8.2"/>
  </g>
</svg>
"###;

const BIPARTITE_GRAPHLET: &str = r###"<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 160 160" role="img" aria-label="Bipartite graph" fill="none">
  <title>Bipartite graph</title>
  <g stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
    <path d="M48 42L112 42" opacity=".78"/>
    <path d="M48 42L112 80" opacity=".78"/>
    <path d="M48 42L112 118" opacity=".78"/>

    <path d="M48 80L112 42" opacity=".78"/>
    <path d="M48 80L112 80" opacity=".78"/>
    <path d="M48 80L112 118" opacity=".78"/>

    <path d="M48 118L112 80" opacity=".78"/>
    <path d="M48 118L112 118" opacity=".78"/>

    <path d="M80 22V138" stroke-dasharray="4 5" opacity=".22"/>
  </g>

  <g fill="var(--node-fill, #F7F4EA)" stroke="currentColor" stroke-width="2.5">
    <circle cx="48" cy="42" r="10"/>
    <circle cx="48" cy="80" r="10"/>
    <circle cx="48" cy="118" r="10"/>
    <circle cx="112" cy="42" r="10"/>
    <circle cx="112" cy="80" r="10"/>
    <circle cx="112" cy="118" r="10"/>
  </g>
</svg>
"###;
