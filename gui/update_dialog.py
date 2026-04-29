"""Themed dialog presenting a new release: changelog, size, action buttons.

Aligns with the dashboard zinc + emerald token system from `gui.theme`.
"""

from __future__ import annotations

from PyQt6.QtCore import Qt, QUrl
from PyQt6.QtGui import QDesktopServices
from PyQt6.QtWidgets import (
    QDialog, QVBoxLayout, QHBoxLayout, QLabel, QPushButton,
    QTextBrowser, QFrame,
)

from gui.theme import COLORS


class UpdateDialog(QDialog):
    """Three-action update prompt: install / remind later / skip version.

    Result codes:
      Accepted (1)  → user chose Update Now
      Rejected (0)  → user closed / Remind Me Later
      2             → user chose Skip This Version
    """

    SKIP = 2

    def __init__(self, info: dict, current_version: str, parent=None):
        super().__init__(parent)
        self._info = info
        self.setWindowTitle("Update available")
        self.setModal(True)
        self.setMinimumSize(520, 420)
        # Inherit app stylesheet (zinc/emerald) for QPushButton, QTextBrowser, etc.
        self.setStyleSheet(
            f"QDialog {{ background-color: {COLORS['base']}; }}"
        )

        layout = QVBoxLayout(self)
        layout.setContentsMargins(20, 18, 20, 16)
        layout.setSpacing(12)

        # ── Header strip: version + size ────────────────────────────────
        header = QFrame()
        header.setStyleSheet(
            f"QFrame {{ background: {COLORS['mantle']}; "
            f"border: 1px solid {COLORS['surface1']}; border-radius: 6px; }}"
        )
        header_l = QHBoxLayout(header)
        header_l.setContentsMargins(14, 10, 14, 10)
        header_l.setSpacing(10)

        dot = QLabel("\u25CF")
        dot.setStyleSheet(
            f"color: {COLORS['accent']}; font-size: 10px; "
            "background: transparent; border: none;"
        )
        dot.setFixedWidth(12)
        header_l.addWidget(dot)

        kicker = QLabel("UPDATE AVAILABLE")
        kicker.setStyleSheet(
            f"color: {COLORS['accent']}; font-size: 9px; font-weight: 700; "
            "letter-spacing: 2px; background: transparent; border: none;"
        )
        header_l.addWidget(kicker)
        header_l.addStretch()

        size_mb = info.get("asset_size", 0) / (1024 * 1024)
        size_lbl = QLabel(f"{size_mb:.1f} MB")
        size_lbl.setStyleSheet(
            f"color: {COLORS['subtext0']}; font-size: 11px; "
            "font-family: 'Cascadia Code', 'Consolas', monospace; "
            "background: transparent; border: none;"
        )
        header_l.addWidget(size_lbl)
        layout.addWidget(header)

        # ── Version line ────────────────────────────────────────────────
        version_row = QHBoxLayout()
        version_row.setSpacing(8)

        from_lbl = QLabel(f"v{current_version}")
        from_lbl.setStyleSheet(
            f"color: {COLORS['overlay0']}; font-size: 13px; "
            "font-family: 'Cascadia Code', 'Consolas', monospace;"
        )
        version_row.addWidget(from_lbl)

        arrow = QLabel("\u2192")
        arrow.setStyleSheet(
            f"color: {COLORS['surface2']}; font-size: 14px;"
        )
        version_row.addWidget(arrow)

        to_lbl = QLabel(f"v{info.get('version', '?')}")
        to_lbl.setStyleSheet(
            f"color: {COLORS['accent']}; font-size: 13px; font-weight: 700; "
            "font-family: 'Cascadia Code', 'Consolas', monospace;"
        )
        version_row.addWidget(to_lbl)
        version_row.addStretch()
        layout.addLayout(version_row)

        # ── Changelog ───────────────────────────────────────────────────
        changelog_lbl = QLabel("WHAT'S NEW")
        changelog_lbl.setStyleSheet(
            f"color: {COLORS['overlay0']}; font-size: 9px; font-weight: 700; "
            "letter-spacing: 2px; margin-top: 4px;"
        )
        layout.addWidget(changelog_lbl)

        body = QTextBrowser()
        body.setOpenExternalLinks(True)
        body.setReadOnly(True)
        body.setStyleSheet(
            f"QTextBrowser {{ background: {COLORS['mantle']}; "
            f"color: {COLORS['subtext1']}; "
            f"border: 1px solid {COLORS['surface1']}; border-radius: 6px; "
            "padding: 10px; font-size: 12px; "
            "font-family: 'Segoe UI Variable', 'Segoe UI', system-ui, sans-serif; }}"
        )
        changelog = (info.get("changelog") or "").strip()
        if changelog:
            # Render basic GitHub markdown — Qt's markdown support handles
            # headings / lists / links / code reasonably well.
            body.setMarkdown(changelog)
        else:
            body.setPlainText("No changelog provided for this release.")
        layout.addWidget(body, stretch=1)

        # ── GitHub link (subtle) ───────────────────────────────────────
        if info.get("html_url"):
            link = QLabel(
                f'<a href="{info["html_url"]}" style="color: {COLORS["subtext0"]};">'
                "View release on GitHub \u2197</a>"
            )
            link.setOpenExternalLinks(False)
            link.linkActivated.connect(
                lambda url: QDesktopServices.openUrl(QUrl(url))
            )
            link.setStyleSheet(
                f"color: {COLORS['subtext0']}; font-size: 11px;"
            )
            layout.addWidget(link)

        # ── Action row ─────────────────────────────────────────────────
        actions = QHBoxLayout()
        actions.setSpacing(8)

        btn_skip = QPushButton("Skip This Version")
        btn_skip.setCursor(Qt.CursorShape.PointingHandCursor)
        btn_skip.clicked.connect(self._on_skip)
        actions.addWidget(btn_skip)

        actions.addStretch()

        btn_later = QPushButton("Remind Me Later")
        btn_later.setCursor(Qt.CursorShape.PointingHandCursor)
        btn_later.clicked.connect(self.reject)
        actions.addWidget(btn_later)

        btn_install = QPushButton("Update Now")
        btn_install.setProperty("class", "primary")
        btn_install.setCursor(Qt.CursorShape.PointingHandCursor)
        btn_install.setDefault(True)
        btn_install.clicked.connect(self.accept)
        actions.addWidget(btn_install)

        layout.addLayout(actions)

    def _on_skip(self):
        self.done(self.SKIP)
