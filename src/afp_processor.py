"""
afp_processor.py
────────────────
Parses AFP (Advanced Function Presentation) binary data streams.

AFP is IBM's page-description language used in mainframe output management.
Documents are structured as a sequence of "Structured Fields" (SFs), each
identified by a 3-byte type code.  This module:

  1. Iterates through all SFs in the binary stream.
  2. Uses BPG / EPG boundaries to delimit pages.
  3. Extracts key-value metadata from TLE (Tag Logical Element) SFs.
  4. Returns a list of AFPPage objects ready for the FieldExtractor.

AFP Structured Field layout
───────────────────────────
  Byte  0:   0x5A  — SF Introducer (mandatory marker)
  Bytes 1-2: SFL   — SF Length (big-endian).
                     Counts from byte-1 to the last byte of the SF
                     (i.e. does NOT include the 0x5A introducer byte,
                      but DOES include the 2-byte length field itself).
                     Minimum SFL = 8  (no data payload).
  Byte  3:   FLG   — Flags
  Bytes 4-6: TYP   — 3-byte type code
  Byte  7:   RSV   — Reserved / padding
  Bytes 8+:  DATA  — Payload  (SFL - 7 bytes)

TLE metadata formats supported
───────────────────────────────
  Format A — Variable-length pairs (MO:DCA standard):
    [1: name_len] [name_len: name] [1: value_type] [2: value_len] [value_len: value]

  Format B — IBM triplet format:
    [1: triplet_len] [1: triplet_id] [variable: triplet_data]
    Triplet ID 0x02 = attribute name
    Triplet ID 0x36 = attribute value

  Format C — Null-terminated KEY=VALUE pairs.
"""

import struct
import logging
from dataclasses import dataclass, field
from typing import Dict, Iterator, List, Optional

logger = logging.getLogger(__name__)


# ── Type Code Constants ───────────────────────────────────────────────────────

class _T:
    """AFP Structured Field type codes (3 bytes each)."""
    BDT = b'\xD3\xA8\xA8'   # Begin Document
    EDT = b'\xD3\xA9\xA8'   # End Document
    BPG = b'\xD3\xA8\xAF'   # Begin Page
    EPG = b'\xD3\xA9\xAF'   # End Page
    TLE = b'\xD3\xA0\x90'   # Tag Logical Element  (metadata carrier)
    NOP = b'\xD3\xEE\xEE'   # No Operation         (sometimes holds metadata)
    BNG = b'\xD3\xA8\xAD'   # Begin Named Page Group
    ENG = b'\xD3\xA9\xAD'   # End Named Page Group

    # Human-readable labels for debug logging
    LABELS: Dict[bytes, str] = {
        BDT: 'BDT', EDT: 'EDT',
        BPG: 'BPG', EPG: 'EPG',
        TLE: 'TLE', NOP: 'NOP',
        BNG: 'BNG', ENG: 'ENG',
    }


# ── Data Classes ──────────────────────────────────────────────────────────────

@dataclass
class _SF:
    """One parsed AFP Structured Field."""
    type_code: bytes
    flags:     int
    data:      bytes

    @property
    def label(self) -> str:
        return _T.LABELS.get(self.type_code, self.type_code.hex().upper())


@dataclass
class AFPPage:
    """One logical page with its associated metadata."""
    page_number: int
    metadata:    Dict[str, str] = field(default_factory=dict)

    def __repr__(self) -> str:
        return f"<AFPPage {self.page_number}: {len(self.metadata)} metadata key(s)>"


# ── AFP Processor ─────────────────────────────────────────────────────────────

class AFPProcessor:
    """
    Parse an AFP binary file and return a list of AFPPage objects.

    Usage:
        processor = AFPProcessor()
        pages     = processor.parse_file('path/to/file.afp')
    """

    SF_INTRODUCER  = 0x5A
    SF_HEADER_SIZE = 8     # 1 (introducer) + 2 (SFL) + 1 (flags) + 3 (type) + 1 (reserved)
    SF_MIN_SFL     = 7     # minimum legal value for the SFL field (no data payload)

    def parse_file(self, filepath: str) -> List[AFPPage]:
        """Read and parse an AFP file from disk."""
        logger.debug("AFP: opening '%s'", filepath)
        with open(filepath, 'rb') as fh:
            data = fh.read()
        return self.parse(data)

    def parse(self, data: bytes) -> List[AFPPage]:
        """
        Parse raw AFP bytes.

        Returns:
            List[AFPPage] — one entry per BPG…EPG block found.
            If no page structure is found the whole document is
            treated as a single page (common for older AFP streams).
        """
        pages:        List[AFPPage]       = []
        doc_metadata: Dict[str, str]      = {}
        current:      Optional[AFPPage]   = None
        page_num:     int                 = 0

        for sf in self._iter_sfs(data):
            logger.debug("  SF: %s  (%d data bytes)", sf.label, len(sf.data))

            if sf.type_code == _T.BPG:
                page_num += 1
                current = AFPPage(
                    page_number=page_num,
                    metadata=dict(doc_metadata),   # inherit document-level metadata
                )

            elif sf.type_code == _T.EPG:
                if current is not None:
                    pages.append(current)
                    current = None

            elif sf.type_code in (_T.TLE, _T.NOP):
                meta = self._parse_tle(sf.data)
                if meta:
                    target = current.metadata if current is not None else doc_metadata
                    target.update(meta)

        # Flush an unclosed page (missing EPG)
        if current is not None:
            logger.warning("AFP: missing EPG — flushing open page %d", current.page_number)
            pages.append(current)

        # No page structure found → single-page document
        if not pages:
            logger.debug("AFP: no BPG/EPG found — treating as single page")
            pages = [AFPPage(page_number=1, metadata=doc_metadata)]

        logger.info("AFP: parsed %d page(s)", len(pages))
        return pages

    # ── Structured Field Iterator ─────────────────────────────────────────────

    def _iter_sfs(self, data: bytes) -> Iterator[_SF]:
        """Yield every Structured Field found in the byte stream."""
        pos   = 0
        total = len(data)

        while pos < total:
            # Locate the next introducer byte
            if data[pos] != self.SF_INTRODUCER:
                pos += 1
                continue

            # Need at least the full fixed header
            if pos + self.SF_HEADER_SIZE > total:
                logger.debug("AFP: truncated header at offset %d", pos)
                break

            sfl      = struct.unpack('>H', data[pos + 1: pos + 3])[0]
            sf_flags = data[pos + 3]
            sf_type  = bytes(data[pos + 4: pos + 7])
            # data[pos + 7] = reserved byte

            if sfl < self.SF_MIN_SFL:
                logger.debug("AFP: invalid SFL=%d at offset %d — skipping", sfl, pos)
                pos += 1
                continue

            # Data payload: bytes 8 … (1 + sfl - 1) inclusive
            data_start = pos + 8
            data_end   = pos + 1 + sfl
            sf_data    = data[data_start: min(data_end, total)]

            yield _SF(type_code=sf_type, flags=sf_flags, data=sf_data)

            pos = data_end

    # ── TLE Data Parsing ──────────────────────────────────────────────────────

    def _parse_tle(self, data: bytes) -> Dict[str, str]:
        """
        Try all known TLE / metadata encoding formats.
        Returns the first non-empty result, or {} if nothing parses.
        """
        if not data:
            return {}

        for parser in (
            self._fmt_variable,
            self._fmt_triplet,
            self._fmt_nullterm,
        ):
            result = parser(data)
            if result:
                logger.debug("AFP: TLE parsed (%d attribute(s)) via %s",
                             len(result), parser.__name__)
                return result

        logger.debug("AFP: TLE data unrecognised (%d bytes)", len(data))
        return {}

    # ── Format A: Variable-length name/value pairs ───────────────────────────

    def _fmt_variable(self, data: bytes) -> Dict[str, str]:
        """
        MO:DCA variable-length format:
          [1: name_len] [name_bytes] [1: value_type] [2: value_len] [value_bytes]
        """
        result: Dict[str, str] = {}
        pos = 0

        while pos < len(data):
            # Name length
            name_len = data[pos]
            pos += 1
            if name_len == 0 or pos + name_len > len(data):
                break

            # Name
            try:
                name = data[pos: pos + name_len].decode('ascii').strip()
            except (UnicodeDecodeError, ValueError):
                return {}   # not valid ASCII — wrong format
            pos += name_len

            # value_type (1 byte, ignored) + value_len (2 bytes)
            if pos + 3 > len(data):
                result[name] = ''
                break

            pos += 1   # skip value_type
            value_len = struct.unpack('>H', data[pos: pos + 2])[0]
            pos += 2

            value_end = pos + value_len
            raw_val   = data[pos: min(value_end, len(data))]
            try:
                value = raw_val.decode('ascii', errors='replace').strip()
            except Exception:
                value = ''
            result[name] = value
            pos = value_end

        return result

    # ── Format B: IBM triplet format ──────────────────────────────────────────

    def _fmt_triplet(self, data: bytes) -> Dict[str, str]:
        """
        IBM triplet format:
          [1: triplet_len] [1: triplet_id] [triplet_len-2: triplet_data]
          ID 0x02 → attribute name
          ID 0x36 → attribute value
        """
        result:       Dict[str, str] = {}
        current_name: Optional[str]  = None
        pos = 0

        while pos + 2 <= len(data):
            t_len = data[pos]
            if t_len < 2 or pos + t_len > len(data):
                break
            t_id   = data[pos + 1]
            t_data = data[pos + 2: pos + t_len]

            if t_id == 0x02:   # attribute name
                try:
                    current_name = t_data.decode('ascii', errors='replace').strip()
                except Exception:
                    current_name = None

            elif t_id == 0x36 and current_name:   # attribute value
                try:
                    result[current_name] = t_data.decode('ascii', errors='replace').strip()
                except Exception:
                    pass

            pos += t_len

        return result

    # ── Format C: Null-terminated KEY=VALUE ───────────────────────────────────

    def _fmt_nullterm(self, data: bytes) -> Dict[str, str]:
        """
        Null-terminated key=value pairs:  KEY=VALUE\x00KEY=VALUE\x00 …
        """
        result: Dict[str, str] = {}
        try:
            text   = data.decode('ascii', errors='replace')
            tokens = [t.strip() for t in text.split('\x00') if t.strip()]
        except Exception:
            return {}

        for token in tokens:
            if '=' in token:
                name, _, value = token.partition('=')
                result[name.strip()] = value.strip()

        return result
