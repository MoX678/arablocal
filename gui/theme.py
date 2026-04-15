"""Dashboard operator theme — taste-skill: dashboards style.

Surface ladder: 5-layer zinc neutral ramp from deep base to interactive hover.
Accent: emerald-400 / single accent, not colorful.
Density: VISUAL_DENSITY 7 — tight spacing, mono data, instrument feel.
Variance: DESIGN_VARIANCE 4 — disciplined, not creative.
Motion: MOTION_INTENSITY 4 — state-driven transitions only.

Anti-patterns avoided:
  - No pure #000 backgrounds (use tuned off-black)
  - No neon, no purple, no cyan glow
  - No rounded-2xl (max 8px radius)
  - No luxury whitespace (this is a tool surface)
  - No card-inside-card nesting
  - No decorative shadows (tinted or none)
"""

# ━━━ Surface Ladder ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 5 graduated background layers: base → mantle → surface0 → surface1 → surface2
# + overlay for muted text / disabled
COLORS = {
    "base":       "#0c0c0e",   # deepest — main window bg
    "mantle":     "#131316",   # sidebar, panels, secondary bg
    "surface0":   "#1a1a1f",   # cards, inputs, table bg
    "surface1":   "#252529",   # borders, dividers, subtle lines
    "surface2":   "#35353b",   # hover borders, interactive edges
    "overlay0":   "#56565e",   # disabled text, muted labels

    # Text ramp (zinc-based)
    "text":       "#e4e4e7",   # primary text — zinc-200
    "subtext0":   "#88888f",   # secondary / labels — zinc-450
    "subtext1":   "#b0b0b6",   # semi-bright — zinc-350

    # Single accent — emerald
    "accent":       "#34d399",  # emerald-400
    "accent_hover": "#5eead4",  # teal-300 (hover shift, not just lighter)
    "accent_muted": "#0a2e22",  # deep emerald tint for selected bg
    "accent_dim":   "#166d4e",  # mid emerald for subtle indicators

    # Semantic (functional, not decorative)
    "green":   "#34d399",
    "red":     "#f87171",
    "yellow":  "#eab308",
    "blue":    "#60a5fa",
    "orange":  "#f97316",
}

# ━━━ Stylesheet ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
STYLESHEET = f"""

/* ── Base Reset ────────────────────────────────────────────────────── */
QMainWindow {{
    background-color: {COLORS['base']};
}}

QWidget {{
    background-color: {COLORS['base']};
    color: {COLORS['text']};
    font-family: "Segoe UI Variable", "Segoe UI", "Inter", system-ui, sans-serif;
    font-size: 13px;
    outline: none;
}}

/* ── Sidebar — darker mantle layer ─────────────────────────────────── */
#sidebar {{
    background-color: {COLORS['mantle']};
    border-right: 1px solid {COLORS['surface1']};
    min-width: 160px;
    max-width: 160px;
}}

#sidebar QPushButton {{
    background-color: transparent;
    color: {COLORS['overlay0']};
    border: none;
    border-radius: 0px;
    padding: 8px 12px;
    text-align: left;
    font-size: 11px;
    font-weight: 600;
    letter-spacing: 0.5px;
    margin: 0px 0px;
}}

#sidebar QPushButton:hover {{
    background-color: {COLORS['surface0']};
    color: {COLORS['text']};
}}

#sidebar QPushButton:checked {{
    background-color: {COLORS['accent_muted']};
    color: {COLORS['accent']};
    font-weight: 700;
    border-left: 2px solid {COLORS['accent']};
    padding-left: 10px;
}}

/* ── Buttons — one system of radius (6px) and density ──────────────── */
QPushButton {{
    background-color: {COLORS['surface0']};
    color: {COLORS['text']};
    border: 1px solid {COLORS['surface1']};
    border-radius: 6px;
    padding: 7px 16px;
    font-weight: 500;
    font-size: 12px;
}}

QPushButton:hover {{
    background-color: {COLORS['surface1']};
    border-color: {COLORS['surface2']};
}}

QPushButton:pressed {{
    background-color: {COLORS['surface2']};
}}

QPushButton:disabled {{
    background-color: {COLORS['mantle']};
    color: {COLORS['overlay0']};
    border-color: {COLORS['surface0']};
}}

/* Primary — emerald solid */
QPushButton[class="primary"] {{
    background-color: {COLORS['accent']};
    color: {COLORS['base']};
    border: none;
    font-weight: 600;
}}

QPushButton[class="primary"]:hover {{
    background-color: {COLORS['accent_hover']};
}}

QPushButton[class="primary"]:pressed {{
    background-color: #10b981;
}}

QPushButton[class="primary"]:disabled {{
    background-color: {COLORS['surface1']};
    color: {COLORS['overlay0']};
}}

/* Danger */
QPushButton[class="danger"] {{
    background-color: #991b1b;
    color: {COLORS['red']};
    border: 1px solid #7f1d1d;
    font-weight: 600;
}}

QPushButton[class="danger"]:hover {{
    background-color: #b91c1c;
}}

/* ── Inputs — same 6px system ──────────────────────────────────────── */
QLineEdit, QSpinBox, QDoubleSpinBox {{
    background-color: {COLORS['surface0']};
    color: {COLORS['text']};
    border: 1px solid {COLORS['surface1']};
    border-radius: 6px;
    padding: 6px 10px;
    font-size: 12px;
    selection-background-color: {COLORS['accent_muted']};
    selection-color: {COLORS['accent']};
}}

QLineEdit:focus, QSpinBox:focus, QDoubleSpinBox:focus {{
    border-color: {COLORS['accent_dim']};
}}

/* ── ComboBox ──────────────────────────────────────────────────────── */
QComboBox {{
    background-color: {COLORS['surface0']};
    color: {COLORS['text']};
    border: 1px solid {COLORS['surface1']};
    border-radius: 6px;
    padding: 6px 10px;
    font-size: 12px;
    min-width: 100px;
}}

QComboBox:focus {{
    border-color: {COLORS['accent_dim']};
}}

QComboBox::drop-down {{
    border: none;
    width: 20px;
}}

QComboBox::down-arrow {{
    image: none;
    border-left: 4px solid transparent;
    border-right: 4px solid transparent;
    border-top: 5px solid {COLORS['subtext0']};
    margin-right: 6px;
}}

QComboBox QAbstractItemView {{
    background-color: {COLORS['surface0']};
    color: {COLORS['text']};
    border: 1px solid {COLORS['surface1']};
    selection-background-color: {COLORS['accent_muted']};
    selection-color: {COLORS['accent']};
    outline: none;
    padding: 2px;
}}

/* ── Checkbox — clean indicator ────────────────────────────────────── */
QCheckBox {{
    spacing: 6px;
    color: {COLORS['text']};
    font-size: 12px;
}}

QCheckBox::indicator {{
    width: 16px;
    height: 16px;
    border-radius: 3px;
    border: 1px solid {COLORS['surface2']};
    background-color: {COLORS['surface0']};
}}

QCheckBox::indicator:checked {{
    background-color: {COLORS['accent']};
    border-color: {COLORS['accent']};
}}

QCheckBox::indicator:hover {{
    border-color: {COLORS['accent_dim']};
}}

/* ── Progress Bar — thin instrument strip ──────────────────────────── */
QProgressBar {{
    background-color: {COLORS['surface0']};
    border: none;
    border-radius: 3px;
    text-align: center;
    color: {COLORS['subtext0']};
    font-size: 10px;
    font-family: "Cascadia Code", "Consolas", monospace;
    height: 6px;
}}

QProgressBar::chunk {{
    background-color: {COLORS['accent']};
    border-radius: 3px;
}}

/* ── Tables — dense instrument surface ─────────────────────────────── */
QTableView, QTableWidget {{
    background-color: {COLORS['surface0']};
    alternate-background-color: {COLORS['mantle']};
    color: {COLORS['text']};
    border: 1px solid {COLORS['surface1']};
    border-radius: 6px;
    gridline-color: {COLORS['surface1']};
    selection-background-color: {COLORS['accent_muted']};
    selection-color: {COLORS['accent']};
    font-size: 12px;
}}

QHeaderView::section {{
    background-color: {COLORS['mantle']};
    color: {COLORS['subtext0']};
    border: none;
    border-bottom: 1px solid {COLORS['surface1']};
    border-right: 1px solid {COLORS['surface1']};
    padding: 6px 8px;
    font-weight: 600;
    font-size: 10px;
    text-transform: uppercase;
    letter-spacing: 0.5px;
}}

/* ── Scrollbars — minimal, not decorative ──────────────────────────── */
QScrollBar:vertical {{
    background: transparent;
    width: 6px;
    margin: 2px;
}}

QScrollBar::handle:vertical {{
    background: {COLORS['surface2']};
    min-height: 24px;
    border-radius: 3px;
}}

QScrollBar::handle:vertical:hover {{
    background: {COLORS['overlay0']};
}}

QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical {{
    height: 0px;
}}

QScrollBar:horizontal {{
    background: transparent;
    height: 6px;
    margin: 2px;
}}

QScrollBar::handle:horizontal {{
    background: {COLORS['surface2']};
    min-width: 24px;
    border-radius: 3px;
}}

QScrollBar::handle:horizontal:hover {{
    background: {COLORS['overlay0']};
}}

QScrollBar::add-line:horizontal, QScrollBar::sub-line:horizontal {{
    width: 0px;
}}

/* ── TextEdit — mono log surface ───────────────────────────────────── */
QTextEdit, QPlainTextEdit {{
    background-color: {COLORS['mantle']};
    color: {COLORS['subtext1']};
    border: 1px solid {COLORS['surface1']};
    border-radius: 6px;
    padding: 8px;
    font-family: "Cascadia Code", "Consolas", monospace;
    font-size: 11px;
    line-height: 1.4;
}}

/* ── GroupBox — subtle section container ────────────────────────────── */
QGroupBox {{
    background-color: {COLORS['surface0']};
    border: 1px solid {COLORS['surface1']};
    border-radius: 6px;
    margin-top: 10px;
    padding-top: 24px;
    font-weight: 600;
    color: {COLORS['subtext0']};
    font-size: 11px;
}}

QGroupBox::title {{
    subcontrol-origin: margin;
    subcontrol-position: top left;
    padding: 2px 10px;
    color: {COLORS['subtext0']};
    font-size: 10px;
    letter-spacing: 0.5px;
}}

/* ── Splitter — 1px divider ────────────────────────────────────────── */
QSplitter::handle {{
    background-color: {COLORS['surface1']};
    width: 1px;
}}

/* ── Status bar — operational strip ────────────────────────────────── */
QStatusBar {{
    background-color: {COLORS['mantle']};
    color: {COLORS['subtext0']};
    border-top: 1px solid {COLORS['surface1']};
    font-size: 11px;
    padding: 2px 8px;
}}

/* ── Tooltip ───────────────────────────────────────────────────────── */
QToolTip {{
    background-color: {COLORS['surface0']};
    color: {COLORS['text']};
    border: 1px solid {COLORS['surface1']};
    border-radius: 4px;
    padding: 4px 8px;
    font-size: 11px;
}}

/* ── Scroll area — transparent wrapper ─────────────────────────────── */
QScrollArea {{
    border: none;
    background: transparent;
}}

/* ── Frame — default transparent ───────────────────────────────────── */
QFrame {{
    border: none;
}}
"""
