"""
field_extractor.py
──────────────────
Extracts metadata field values from a single Page using the FieldRule
definitions produced by PolicyParser.

Extraction priority for each field:
  1. replacetext('…')  → hardcoded literal value
  2. metadata('KEY')   → value from file-system / runtime metadata
  3. rowcol(R, C)      → positional character extraction from the page

Anchor logic:
  - The anchor field has matches('VALUE') and a rowcol position.
  - Every page is scanned for a line where the value at that column
    equals the matches string.
  - If the anchor is not found the page is skipped (returns None).
  - All fields with follows(SAMELINE, anchor) are extracted from the
    same line as the anchor using only the column from their rowcol.
"""

import os
import logging
from datetime import datetime
from typing import Dict, List, Optional

from policy_parser import FieldRule, PolicyConfig
from doc_processor import Page

logger = logging.getLogger(__name__)


class FieldExtractor:
    """Extract field values from a Page using FieldRule definitions."""

    def __init__(self, config: PolicyConfig, field_rules: Dict[str, FieldRule]):
        self.config      = config
        self.field_rules = field_rules
        self._anchor: Optional[FieldRule] = self._find_anchor_rule()

        if self._anchor:
            logger.debug("Anchor field: '%s' matches('%s') at rowcol(%s, %s)",
                         self._anchor.name, self._anchor.matches,
                         self._anchor.row,  self._anchor.col)
        else:
            logger.warning("No anchor field defined in policy.")

    # ── Public API ────────────────────────────────────────────────────────────

    def extract_page(
        self,
        page: Page,
        file_path: str
    ) -> Optional[Dict[str, str]]:
        """
        Extract all field values from *page*.

        Returns:
            Dict[field_name → value]  if the page matches the anchor.
            None                       if the anchor was not found.
        """
        # ── Step 1: locate the anchor line ───────────────────────────────────
        anchor_idx: Optional[int] = None
        if self._anchor:
            anchor_idx = self._find_anchor_line(page)
            if anchor_idx is None:
                logger.debug("Page %d: anchor not found — skipping.", page.page_number)
                return None
            logger.debug("Page %d: anchor found at line index %d.",
                         page.page_number, anchor_idx)

        # ── Step 2: extract every field ───────────────────────────────────────
        results: Dict[str, str] = {}

        for name, rule in self.field_rules.items():
            if rule.is_anchor:
                results[name] = rule.matches or ''
                continue

            value = self._extract_field(rule, page, anchor_idx, file_path)

            if value is not None:
                results[name] = value
            elif rule.allow_blank:
                results[name] = ''
            else:
                logger.debug("Page %d: field '%s' not extracted.",
                             page.page_number, name)

        return results

    # ── Anchor Detection ──────────────────────────────────────────────────────

    def _find_anchor_rule(self) -> Optional[FieldRule]:
        for rule in self.field_rules.values():
            if rule.is_anchor:
                return rule
        return None

    def _find_anchor_line(self, page: Page) -> Optional[int]:
        """
        Scan every line in the page looking for the anchor match value
        at the expected column position.
        """
        rule   = self._anchor
        target = rule.matches or ''
        col    = rule.col or 1
        length = len(target)

        for idx, line in enumerate(page.lines):
            extracted = self._chars_at(line, col, length)
            if extracted == target:
                return idx
        return None

    # ── Field Dispatch ────────────────────────────────────────────────────────

    def _extract_field(
        self,
        rule:           FieldRule,
        page:           Page,
        anchor_idx:     Optional[int],
        file_path:      str,
    ) -> Optional[str]:
        """
        Choose the correct extraction strategy for *rule*.
        Priority: replacetext → metadata → positional.
        """
        # 1. Hardcoded value
        if rule.replace_text is not None:
            return self._format(rule.replace_text, rule)

        # 2. Metadata value
        if rule.metadata_key:
            raw = self._resolve_metadata(rule, file_path)
            return self._format(raw, rule)

        # 3. Positional extraction
        if rule.row is not None and rule.col is not None:
            return self._extract_positional(rule, page, anchor_idx)

        return None

    # ── Positional Extraction ─────────────────────────────────────────────────

    def _extract_positional(
        self,
        rule:       FieldRule,
        page:       Page,
        anchor_idx: Optional[int],
    ) -> Optional[str]:
        """Extract characters from a specific line/column in the page."""

        line_idx = self._resolve_line_index(rule, anchor_idx)
        if line_idx is None or line_idx >= len(page.lines):
            logger.debug("Field '%s': line index %s out of range (page has %d lines).",
                         rule.name, line_idx, len(page.lines))
            return '' if rule.allow_blank else None

        line   = page.lines[line_idx]
        length = rule.max_length or 50
        raw    = self._chars_at(line, rule.col, length).strip()

        if not raw and not rule.allow_blank:
            return None

        return self._format(raw, rule)

    def _resolve_line_index(
        self,
        rule:       FieldRule,
        anchor_idx: Optional[int],
    ) -> Optional[int]:
        """
        Map a FieldRule's follows / rowcol to a 0-based line index.

        follows(SAMELINE, anchor)  → use the anchor's exact line.
        follows(NEXTLINE, anchor)  → anchor line + 1.
        No follows                 → row is absolute (1-based from page top).
        """
        ft = (rule.follows_type or '').upper()

        if ft == 'SAMELINE' and anchor_idx is not None:
            return anchor_idx
        elif ft == 'NEXTLINE' and anchor_idx is not None:
            return anchor_idx + 1
        elif rule.row is not None:
            return rule.row - 1   # convert 1-based → 0-based
        return None

    # ── Character-Level Helpers ───────────────────────────────────────────────

    @staticmethod
    def _chars_at(line: str, col: int, length: int) -> str:
        """
        Extract *length* characters from *line* starting at 1-based column *col*.

        Column numbering follows Mobius convention:
          col 1 = index 0  (ANSI CC char when AnsiCC mode is active)
          col 2 = index 1  (first printable character)

        If the line is shorter than required the result is space-padded.
        """
        start   = col - 1
        end     = start + length
        segment = line[start:end] if start < len(line) else ''
        return segment.ljust(length)

    # ── Metadata Resolver ─────────────────────────────────────────────────────

    def _resolve_metadata(self, rule: FieldRule, file_path: str) -> str:
        """Map a metadata('KEY') reference to its runtime value."""
        key      = rule.metadata_key
        abs_path = os.path.abspath(file_path)

        dispatch = {
            'FILE_NAME':     lambda: os.path.basename(abs_path),
            'FILE_DIR1':     lambda: os.path.basename(os.path.dirname(abs_path)),
            'FILE_DIR2':     lambda: os.path.basename(
                                         os.path.dirname(os.path.dirname(abs_path))),
            'FILE_DATE':     lambda: self._file_date(abs_path, rule),
            'CURRENT_DATE':  lambda: datetime.now().strftime('%Y%m%d'),
            'ESTATEMENTKEY': lambda: os.path.splitext(os.path.basename(abs_path))[0],
        }

        handler = dispatch.get(key)
        if handler:
            return handler()

        logger.warning("Unknown metadata key: '%s'", key)
        return ''

    def _file_date(self, file_path: str, rule: FieldRule) -> str:
        """Return the file modification date formatted per rule.date_format."""
        try:
            mtime = os.path.getmtime(file_path)
            dt    = datetime.fromtimestamp(mtime)
            if rule.date_format:
                fmt = (rule.date_format
                       .replace('YYYY', '%Y')
                       .replace('MM',   '%m')
                       .replace('DD',   '%d'))
                return dt.strftime(fmt)
            return dt.strftime('%Y%m%d')
        except OSError as exc:
            logger.warning("Cannot stat '%s': %s", file_path, exc)
            return ''

    # ── Formatting / Padding ──────────────────────────────────────────────────

    @staticmethod
    def _format(value: str, rule: FieldRule) -> str:
        """Apply max-length truncation and space-padding to *value*."""
        if value is None:
            value = ''

        # Truncate to max_length
        if rule.max_length:
            value = value[:rule.max_length]

        # Right-pad to pad_length
        if rule.pad_length and len(value) < rule.pad_length:
            value = value.ljust(rule.pad_length, rule.pad_char)

        return value
