"""
doc_processor.py
────────────────
Reads a document file and splits it into Page objects using
the page-break and line-break rules from the policy.

Supported page-break modes:
  AnsiCC    — first byte of each line is an ANSI Carriage Control char;
              '1' means "skip to top of new page".
  FormFeed  — ASCII form-feed character (\f) separates pages.
  None      — whole file is one page.
"""

import logging
from dataclasses import dataclass, field
from typing import List

from policy_parser import PolicyConfig

logger = logging.getLogger(__name__)


# ── Data Classes ──────────────────────────────────────────────────────────────

@dataclass
class Page:
    """One logical page inside a document."""
    page_number: int
    lines: List[str] = field(default_factory=list)

    def get_line(self, row: int) -> str:
        """
        Return the line at 1-based row number.
        Returns an empty string if the row is out of bounds.
        """
        idx = row - 1
        return self.lines[idx] if 0 <= idx < len(self.lines) else ''

    def __repr__(self) -> str:
        return f"<Page {self.page_number}: {len(self.lines)} line(s)>"


# ── Processor ─────────────────────────────────────────────────────────────────

class DocumentProcessor:
    """Reads a document file and returns a list of Page objects."""

    # ANSI CC chars and their meaning
    _ANSI_CC = {
        '1': 'new_page',
        ' ': 'single_space',
        '0': 'double_space',
        '-': 'triple_space',
        '+': 'overprint',
    }

    def __init__(self, config: PolicyConfig):
        self.config = config

    # ── Public API ────────────────────────────────────────────────────────────

    def process_file(self, filepath: str) -> List[Page]:
        """Read a document file from disk and split into pages."""
        encoding = self._resolve_encoding()
        logger.debug("Reading '%s' (encoding=%s)", filepath, encoding)

        with open(filepath, 'r', encoding=encoding, errors='replace') as fh:
            content = fh.read()

        return self.process_text(content)

    def process_text(self, content: str) -> List[Page]:
        """Split raw text content into pages."""
        lines = self._split_lines(content)
        pages = self._split_pages(lines)
        logger.info("Document: %d line(s) → %d page(s)", len(lines), len(pages))
        return pages

    # ── Encoding ──────────────────────────────────────────────────────────────

    def _resolve_encoding(self) -> str:
        cs = self.config.char_set.upper()
        return {
            'ASCII':  'ascii',
            'UTF8':   'utf-8',
            'UTF-8':  'utf-8',
            'EBCDIC': 'cp500',
        }.get(cs, 'ascii')

    # ── Line Splitting ────────────────────────────────────────────────────────

    def _split_lines(self, content: str) -> List[str]:
        """Split content into individual lines per the LineBreaks setting."""
        lb = self.config.line_breaks.upper()
        if lb == 'CRLF':
            return content.replace('\r\n', '\n').splitlines()
        elif lb == 'CR':
            return content.split('\r')
        else:
            return content.splitlines()

    # ── Page Splitting ────────────────────────────────────────────────────────

    def _split_pages(self, lines: List[str]) -> List[Page]:
        pb = self.config.page_breaks.upper()
        if pb == 'ANSICC':
            return self._split_ansi_cc(lines)
        elif pb == 'FORMFEED':
            return self._split_form_feed(lines)
        else:
            return [Page(page_number=1, lines=lines)]

    def _split_ansi_cc(self, lines: List[str]) -> List[Page]:
        """
        ANSI Carriage Control page splitting.

        Rules:
          • The VERY FIRST character of every line is the CC byte.
          • '1'  → skip to top of form  (= start of new page).
          • All other CC chars belong to the current page.
          • The CC character is RETAINED in the line so that column-based
            extraction matches Mobius rowcol semantics:
              col 1 = CC char, col 2 = first printable character.
        """
        pages:        List[Page] = []
        current_lines: List[str] = []
        page_num:      int       = 1

        for raw_line in lines:
            cc = raw_line[0] if raw_line else ' '
            if cc == '1':
                if current_lines:
                    pages.append(Page(page_number=page_num, lines=current_lines))
                    page_num += 1
                current_lines = [raw_line]
            else:
                current_lines.append(raw_line)

        if current_lines:
            pages.append(Page(page_number=page_num, lines=current_lines))

        if not pages:
            pages = [Page(page_number=1, lines=lines)]

        return pages

    def _split_form_feed(self, lines: List[str]) -> List[Page]:
        """Split pages on ASCII form-feed character (\\f)."""
        pages:        List[Page] = []
        current_lines: List[str] = []
        page_num:      int       = 1

        for line in lines:
            if '\f' in line:
                parts = line.split('\f')
                current_lines.append(parts[0])
                pages.append(Page(page_number=page_num, lines=current_lines))
                page_num      += 1
                current_lines  = [parts[1]] if len(parts) > 1 else []
            else:
                current_lines.append(line)

        if current_lines:
            pages.append(Page(page_number=page_num, lines=current_lines))

        return pages
