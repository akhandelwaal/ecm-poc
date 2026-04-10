# “””
AFP (Advanced Function Presentation) File Parser

AFP is an IBM proprietary binary format (MO:DCA-P / IPDS architecture).
An AFP file is a stream of “structured fields”, each with:

┌──────────┬────────┬──────────┬──────────────────────┐
│ Carriage  │ Length │ SF Type  │  Data / Payload      │
│ Control   │ (2 B)  │ (3 B)   │  (variable)          │
│ (1 byte)  │        │          │                      │
└──────────┴────────┴──────────┴──────────────────────┘

- Carriage control: 0x5A (constant sentinel for every structured field)
- Length: 2-byte big-endian uint — total length of the SF *including*
  the length field itself but *excluding* the 0x5A byte
- SF Type: 3-byte identifier (e.g., 0xD3A8A8 = Begin Document)
- Payload: remaining bytes

This parser extracts structured fields, decodes known types,
pulls text content (from PTX / Presentation Text), and collects
metadata from document-level structured fields.

References:

- MO:DCA Reference (IBM SC31-6802)
- AFP Programming Guide (IBM G544-3876)
  “””

import struct
import logging
from pathlib import Path
from dataclasses import dataclass, field
from typing import BinaryIO

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(**name**)

# —————————————————————————

# AFP Structured Field Type Registry

# —————————————————————————

# Format: 3-byte code -> (abbreviation, full_name)

# Only the most common types are listed; extend as needed.

SF_TYPES: dict[bytes, tuple[str, str]] = {
# Document-level
b”\xD3\xA8\xA8”: (“BDT”, “Begin Document”),
b”\xD3\xA9\xA8”: (“EDT”, “End Document”),
# Page-level
b”\xD3\xA8\xAF”: (“BPG”, “Begin Page”),
b”\xD3\xA9\xAF”: (“EPG”, “End Page”),
# Active Environment Group
b”\xD3\xA8\xC9”: (“BAG”, “Begin Active Environment Group”),
b”\xD3\xA9\xC9”: (“EAG”, “End Active Environment Group”),
# Presentation Text
b”\xD3\xA8\x9B”: (“BPT”, “Begin Presentation Text Object”),
b”\xD3\xA9\x9B”: (“EPT”, “End Presentation Text Object”),
b”\xD3\xEE\x9B”: (“PTX”, “Presentation Text Data”),
# Page Descriptor
b”\xD3\xA6\xAF”: (“PGD”, “Page Descriptor”),
# Map
b”\xD3\xAB\x8A”: (“MCF”, “Map Coded Font”),
# Coded Font
b”\xD3\xA8\x8A”: (“BCF”, “Begin Coded Font”),
b”\xD3\xA9\x8A”: (“ECF”, “End Coded Font”),
# Image
b”\xD3\xA8\xFB”: (“BII”, “Begin Image Object”),
b”\xD3\xA9\xFB”: (“EII”, “End Image Object”),
b”\xD3\xEE\xFB”: (“IPD”, “Image Picture Data”),
# Resource
b”\xD3\xA8\xCE”: (“BRS”, “Begin Resource”),
b”\xD3\xA9\xCE”: (“ERS”, “End Resource”),
# Named Page Group
b”\xD3\xA8\xAD”: (“BNG”, “Begin Named Page Group”),
b”\xD3\xA9\xAD”: (“ENG”, “End Named Page Group”),
# Tag Logical Element (TLE) — key metadata carrier
b”\xD3\xA0\x90”: (“TLE”, “Tag Logical Element”),
# No Operation
b”\xD3\xEE\xEE”: (“NOP”, “No Operation”),
# Include Page Segment
b”\xD3\xAF\x5F”: (“IPS”, “Include Page Segment”),
}

# EBCDIC codepage 500 — used by many AFP text fields

# For full EBCDIC support use the ‘ebcdic’ package; this covers printable ASCII range.

EBCDIC_TO_ASCII = {}
try:
# Build a basic EBCDIC cp500 -> unicode mapping
for i in range(256):
try:
EBCDIC_TO_ASCII[i] = bytes([i]).decode(“cp500”)
except (UnicodeDecodeError, ValueError):
EBCDIC_TO_ASCII[i] = “”
except Exception:
pass

def decode_ebcdic(data: bytes, codepage: str = “cp500”) -> str:
“”“Decode EBCDIC bytes to a unicode string.”””
try:
return data.decode(codepage)
except (UnicodeDecodeError, LookupError):
# Fallback: character-by-character
return “”.join(EBCDIC_TO_ASCII.get(b, “”) for b in data)

def try_decode_text(data: bytes) -> str:
“”“Try multiple encodings to decode text from AFP payload.”””
for encoding in (“cp500”, “cp1252”, “utf-8”, “latin-1”):
try:
return data.decode(encoding)
except (UnicodeDecodeError, LookupError):
continue
return data.hex()

# —————————————————————————

# Data Classes

# —————————————————————————

@dataclass
class StructuredField:
“”“A single AFP structured field.”””
offset: int                 # byte offset in the file
sf_type_bytes: bytes        # 3-byte type code
sf_abbreviation: str        # e.g. “PTX”, “BDT”
sf_name: str                # e.g. “Presentation Text Data”
length: int                 # total SF length (from the length field)
flags: int                  # flag byte (first byte of the intro after type)
payload: bytes              # raw data payload
decoded_text: str = “”      # decoded text content (if applicable)

@dataclass
class AFPPage:
“”“Represents a single page in the AFP document.”””
page_number: int
structured_fields: list[StructuredField] = field(default_factory=list)
text_content: str = “”

@dataclass
class AFPDocument:
“”“Top-level parsed AFP document.”””
filename: str
total_structured_fields: int = 0
pages: list[AFPPage] = field(default_factory=list)
metadata: dict = field(default_factory=dict)
all_text: str = “”
tag_logical_elements: dict = field(default_factory=dict)
structured_fields: list[StructuredField] = field(default_factory=list)
warnings: list[str] = field(default_factory=list)

# —————————————————————————

# PTX (Presentation Text Data) Sub-Parser

# —————————————————————————

def extract_text_from_ptx(payload: bytes) -> str:
“””
Extract readable text from a PTX (Presentation Text Data) structured field.

```
PTX payloads contain a series of control sequences (chained or unchained)
intermixed with text runs. Control sequences start with 0x2B (escape),
followed by a length byte and a function type. Text runs are the bytes
between control sequences.

This is a best-effort extractor; complex PTX with inline direction
changes or DBCS will need more sophisticated handling.
"""
text_parts = []
i = 0
data_len = len(payload)

while i < data_len:
    byte = payload[i]

    if byte == 0x2B and (i + 2) < data_len:
        # Control sequence: 0x2B + length + function_type + params
        cs_length = payload[i + 1]
        # Skip the entire control sequence
        i += cs_length if cs_length > 0 else 2
        continue

    # Treat as text data
    text_run_start = i
    while i < data_len and payload[i] != 0x2B:
        i += 1

    text_run = payload[text_run_start:i]
    if text_run:
        decoded = try_decode_text(text_run)
        # Filter out non-printable noise
        cleaned = "".join(c for c in decoded if c.isprintable() or c in ("\n", "\r", "\t"))
        if cleaned.strip():
            text_parts.append(cleaned)

return " ".join(text_parts)
```

# —————————————————————————

# TLE (Tag Logical Element) Parser — The key metadata carrier

# —————————————————————————

def parse_tle(payload: bytes) -> tuple[str, str]:
“””
Parse a Tag Logical Element (TLE) structured field.

```
TLE carries metadata as name-value pairs (triplets).
Structure after the 2-byte flags:
    - Triplet(s), each with:
        - 1 byte: triplet length
        - 1 byte: triplet ID (0x02 = Fully Qualified Name, 0x36 = Attribute Value)
        - N bytes: data

Returns (attribute_name, attribute_value).
"""
attr_name = ""
attr_value = ""

i = 0
while i < len(payload):
    if i + 2 > len(payload):
        break

    triplet_len = payload[i]
    if triplet_len < 2 or i + triplet_len > len(payload):
        break

    triplet_id = payload[i + 1]
    triplet_data = payload[i + 2 : i + triplet_len]

    if triplet_id == 0x02:
        # Fully Qualified Name — the attribute name
        # First 2 bytes are type/format; remaining is the name
        if len(triplet_data) > 2:
            attr_name = try_decode_text(triplet_data[2:]).strip().strip("\x00")
    elif triplet_id == 0x36:
        # Attribute Value
        if len(triplet_data) > 2:
            attr_value = try_decode_text(triplet_data[2:]).strip().strip("\x00")

    i += triplet_len

return attr_name, attr_value
```

# —————————————————————————

# BDT (Begin Document) Parser

# —————————————————————————

def parse_bdt(payload: bytes) -> dict:
“”“Extract document name and info from Begin Document structured field.”””
info = {}
if len(payload) >= 8:
doc_name = try_decode_text(payload[:8]).strip()
if doc_name:
info[“document_name”] = doc_name
return info

# —————————————————————————

# Main AFP Parser

# —————————————————————————

def convert_text_dump_to_binary(text_dump: str) -> bytes:
“””
Convert a text dump of an AFP file (as seen in Notepad++ or Python repr)
back into raw binary bytes.

```
Handles formats like:
  - Python byte-string repr:  b'\x5a\x00\x8d\xd3...'
  - Escaped hex in plain text: \x5a\x00\x8d\xd3...
  - Space-separated hex:       5A 00 8D D3 ...
"""
import ast
import re

text_dump = text_dump.strip()

# Case 1: Python byte literal — b'...' or b"..."
if text_dump.startswith(("b'", 'b"')):
    try:
        return ast.literal_eval(text_dump)
    except (ValueError, SyntaxError):
        pass

# Case 2: Continuous \xHH escaped string
if "\\x" in text_dump:
    # Remove any b' prefix/suffix artifacts
    text_dump = re.sub(r"^b['\"]|['\"]$", "", text_dump)
    try:
        return text_dump.encode("utf-8").decode("unicode_escape").encode("latin-1")
    except (UnicodeDecodeError, ValueError):
        pass
    # Fallback: extract all \xHH patterns
    hex_values = re.findall(r"\\x([0-9a-fA-F]{2})", text_dump)
    if hex_values:
        return bytes(int(h, 16) for h in hex_values)

# Case 3: Space-separated hex bytes (e.g., "5A 00 8D D3")
hex_match = re.findall(r"[0-9a-fA-F]{2}", text_dump)
if hex_match and len(hex_match) > 10:
    return bytes(int(h, 16) for h in hex_match)

raise ValueError("Could not detect the text dump format. Provide raw binary AFP file instead.")
```

class AFPParser:
“””
Parser for AFP (Advanced Function Presentation) binary files.

```
Usage:
    # From a raw binary .afp file:
    parser = AFPParser("path/to/file.afp")
    doc = parser.parse()

    # From a text dump (e.g., Notepad++ hex view or Python byte repr):
    parser = AFPParser("path/to/dump.txt", is_text_dump=True)
    doc = parser.parse()

    print(doc.all_text)
    print(doc.metadata)
    print(doc.tag_logical_elements)
"""

SENTINEL = 0x5A

def __init__(self, filepath: str | Path, is_text_dump: bool = False):
    self.filepath = Path(filepath)
    self.is_text_dump = is_text_dump
    if not self.filepath.exists():
        raise FileNotFoundError(f"AFP file not found: {self.filepath}")

def _get_binary_stream(self) -> BinaryIO:
    """
    Returns a binary stream for the AFP data.
    If the file is a text dump, converts it to binary first.
    """
    import io

    if self.is_text_dump:
        with open(self.filepath, "r", encoding="utf-8", errors="replace") as f:
            text_content = f.read()
        raw_bytes = convert_text_dump_to_binary(text_content)
        logger.info(f"Converted text dump to {len(raw_bytes)} raw bytes")
        return io.BytesIO(raw_bytes)
    else:
        return open(self.filepath, "rb")

def parse(self) -> AFPDocument:
    """Parse the AFP file and return an AFPDocument."""
    doc = AFPDocument(filename=self.filepath.name)

    stream = self._get_binary_stream()
    try:
        self._parse_stream(stream, doc)
    finally:
        stream.close()

    # Consolidate all page text
    doc.all_text = "\n\n".join(
        page.text_content for page in doc.pages if page.text_content
    )

    logger.info(
        f"Parsed {doc.total_structured_fields} structured fields, "
        f"{len(doc.pages)} pages, "
        f"{len(doc.tag_logical_elements)} TLE metadata entries"
    )
    return doc

def _parse_stream(self, f: BinaryIO, doc: AFPDocument):
    """Walk through the binary stream extracting structured fields."""
    current_page: AFPPage | None = None
    page_counter = 0

    while True:
        # --- Find the next 0x5A sentinel ---
        sentinel_byte = f.read(1)
        if not sentinel_byte:
            break  # EOF

        if sentinel_byte[0] != self.SENTINEL:
            # Skip non-sentinel bytes (padding, line breaks, etc.)
            continue

        offset = f.tell() - 1

        # --- Read length (2 bytes, big-endian) ---
        length_bytes = f.read(2)
        if len(length_bytes) < 2:
            break
        sf_length = struct.unpack(">H", length_bytes)[0]

        # --- Read the SF introducer + data ---
        # sf_length includes the 2 length bytes themselves,
        # so remaining data = sf_length - 2
        remaining = sf_length - 2
        if remaining < 3:
            doc.warnings.append(f"SF at offset {offset} too short (length={sf_length})")
            f.read(max(remaining, 0))
            continue

        sf_data = f.read(remaining)
        if len(sf_data) < remaining:
            doc.warnings.append(f"Truncated SF at offset {offset}")
            break

        # --- Parse the 3-byte type code ---
        sf_type_bytes = sf_data[0:3]
        flags = sf_data[3] if len(sf_data) > 3 else 0
        payload = sf_data[4:] if len(sf_data) > 4 else b""

        # Look up type
        type_info = SF_TYPES.get(sf_type_bytes, ("UNK", f"Unknown ({sf_type_bytes.hex()})"))
        abbr, name = type_info

        sf = StructuredField(
            offset=offset,
            sf_type_bytes=sf_type_bytes,
            sf_abbreviation=abbr,
            sf_name=name,
            length=sf_length,
            flags=flags,
            payload=payload,
        )

        doc.total_structured_fields += 1
        doc.structured_fields.append(sf)

        # --- Handle specific structured field types ---

        if abbr == "BDT":
            # Begin Document — extract doc-level metadata
            doc.metadata.update(parse_bdt(payload))

        elif abbr == "BPG":
            # Begin Page
            page_counter += 1
            current_page = AFPPage(page_number=page_counter)
            doc.pages.append(current_page)

        elif abbr == "EPG":
            # End Page
            if current_page:
                current_page = None

        elif abbr == "PTX":
            # Presentation Text — extract text
            text = extract_text_from_ptx(payload)
            sf.decoded_text = text
            if current_page:
                current_page.text_content += text + " "
                current_page.structured_fields.append(sf)

        elif abbr == "TLE":
            # Tag Logical Element — key=value metadata
            attr_name, attr_value = parse_tle(payload)
            if attr_name:
                doc.tag_logical_elements[attr_name] = attr_value
                sf.decoded_text = f"{attr_name}={attr_value}"

        elif abbr == "NOP":
            # No Operation — sometimes carries comments
            nop_text = try_decode_text(payload).strip()
            if nop_text:
                sf.decoded_text = nop_text
                doc.metadata.setdefault("nop_comments", []).append(nop_text)

def dump_structure(self, max_fields: int = 200) -> str:
    """
    Parse and return a human-readable dump of the structured fields.
    Useful for debugging / understanding an AFP file's layout.
    """
    doc = self.parse()
    lines = [
        f"AFP File: {doc.filename}",
        f"Total Structured Fields: {doc.total_structured_fields}",
        f"Pages: {len(doc.pages)}",
        f"Metadata: {doc.metadata}",
        f"TLE Tags: {doc.tag_logical_elements}",
        "",
        f"{'Offset':>10}  {'Abbr':<5}  {'Length':>6}  {'Name':<40}  {'Text Preview'}",
        "-" * 100,
    ]
    for sf in doc.structured_fields[:max_fields]:
        preview = sf.decoded_text[:60].replace("\n", "\\n") if sf.decoded_text else ""
        lines.append(
            f"{sf.offset:>10}  {sf.sf_abbreviation:<5}  {sf.length:>6}  "
            f"{sf.sf_name:<40}  {preview}"
        )

    if doc.total_structured_fields > max_fields:
        lines.append(f"\n... ({doc.total_structured_fields - max_fields} more fields)")

    return "\n".join(lines)
```

# —————————————————————————

# Convenience Functions

# —————————————————————————

def parse_afp(filepath: str | Path) -> AFPDocument:
“”“One-liner to parse an AFP file.”””
return AFPParser(filepath).parse()

def extract_text(filepath: str | Path) -> str:
“”“Extract all text content from an AFP file.”””
return parse_afp(filepath).all_text

def extract_metadata(filepath: str | Path) -> dict:
“”“Extract metadata (TLE tags + document info) from an AFP file.”””
doc = parse_afp(filepath)
return {
“document_metadata”: doc.metadata,
“tag_logical_elements”: doc.tag_logical_elements,
“page_count”: len(doc.pages),
}

# —————————————————————————

# CLI Entry Point

# —————————————————————————

if **name** == “**main**”:
import sys

```
if len(sys.argv) < 2:
    print("Usage: python afp_parser.py <file.afp> [--dump | --text | --metadata] [--text-dump]")
    print()
    print("Modes:")
    print("  --dump       Show structured field layout (default)")
    print("  --text       Extract all text content")
    print("  --metadata   Extract metadata as JSON")
    print()
    print("Options:")
    print("  --text-dump  File is a text/hex dump, not raw binary")
    sys.exit(1)

afp_file = sys.argv[1]
args = sys.argv[2:]
is_text_dump = "--text-dump" in args
mode = "--dump"
for a in args:
    if a in ("--dump", "--text", "--metadata"):
        mode = a

parser = AFPParser(afp_file, is_text_dump=is_text_dump)

if mode == "--dump":
    print(parser.dump_structure())
elif mode == "--text":
    doc = parser.parse()
    print(doc.all_text)
elif mode == "--metadata":
    import json
    meta = extract_metadata(afp_file)
    print(json.dumps(meta, indent=2))
else:
    print(f"Unknown mode: {mode}")
    print("Use --dump, --text, or --metadata")
```
