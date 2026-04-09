"""
policy_parser.py
────────────────
Parses Mobius-style .policy files into three structured objects:
  - PolicyConfig  : document-level settings  (policy block)
  - FieldRule     : per-field extraction rule (field block)
  - IndexRule     : index / search key def   (index block)
"""

import re
import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)


# ── Data Classes ──────────────────────────────────────────────────────────────

@dataclass
class PolicyConfig:
    """Mirrors the 'policy' block — how to read the document file."""
    description:    str = ''
    sample_file:    str = ''
    data_type:      str = 'Text'
    char_set:       str = 'ASCII'
    page_breaks:    str = 'AnsiCC'
    line_breaks:    str = 'CRLF'
    page_view_mode: str = 'TextOnly'
    # AFP-specific
    formdef:        str = ''
    input_res_path: str = ''


@dataclass
class FieldRule:
    """Mirrors one entry in the 'field' block — how to extract a value."""
    name:          str

    # Type
    level:         int           = 1
    data_type:     str           = 'string'   # 'string' | 'date'
    max_length:    Optional[int] = None
    date_format:   Optional[str] = None

    # Formatting
    pad_length:    Optional[int] = None
    pad_char:      str           = ' '

    # Behaviour flags
    single_instance: bool = False
    allow_blank:     bool = False

    # Positional extraction
    row:           Optional[int] = None
    col:           Optional[int] = None

    # Relative positioning
    follows_type:  Optional[str] = None   # e.g. 'SAMELINE'
    follows_field: Optional[str] = None   # e.g. 'anchor'

    # Anchor detection
    matches:       Optional[str] = None   # literal value to match
    is_anchor:     bool          = False

    # Override / metadata
    replace_text:      Optional[str] = None   # hardcode this value
    metadata_key:      Optional[str] = None   # pull from file/runtime metadata
    # AFP-specific
    format_conversion: Optional[int] = None   # Mobius format conversion code


@dataclass
class IndexRule:
    """Mirrors one entry in the 'index' block — a named search key."""
    name:              str
    group_usage:       Optional[int] = None
    group_persistence: bool          = False
    fields:            List[str]     = field(default_factory=list)


# ── Parser ────────────────────────────────────────────────────────────────────

class PolicyParser:
    """Parse a Mobius .policy file into (PolicyConfig, fields, indexes)."""

    # ── Public API ────────────────────────────────────────────────────────────

    def parse_file(self, filepath: str):
        """Load and parse a .policy file from disk."""
        with open(filepath, 'r', encoding='utf-8') as fh:
            content = fh.read()
        return self.parse(content)

    def parse(self, policy_text: str):
        """
        Parse raw policy text.
        Returns:
            (PolicyConfig,
             Dict[str, FieldRule],
             Dict[str, IndexRule])
        """
        sections = self._split_sections(policy_text)
        config  = self._parse_policy_section(sections.get('policy', ''))
        fields  = self._parse_field_section(sections.get('field',  ''))
        indexes = self._parse_index_section(sections.get('index',  ''))
        logger.info(
            "Policy parsed — %d field(s), %d index(es)",
            len(fields), len(indexes)
        )
        return config, fields, indexes

    # ── Section Splitter ──────────────────────────────────────────────────────

    def _split_sections(self, text: str) -> Dict[str, str]:
        """Split the file into its three top-level keyword sections."""
        sections: Dict[str, str] = {}
        current: Optional[str]   = None
        buf: List[str]           = []

        for line in text.splitlines():
            stripped = line.strip()
            if stripped in ('policy', 'field', 'index'):
                if current is not None:
                    sections[current] = '\n'.join(buf)
                current = stripped
                buf = []
            elif current is not None:
                buf.append(line)

        if current is not None:
            sections[current] = '\n'.join(buf)

        return sections

    # ── Policy Block ──────────────────────────────────────────────────────────

    def _parse_policy_section(self, text: str) -> PolicyConfig:
        """Parse the 'policy' block into a PolicyConfig dataclass."""
        config = PolicyConfig()
        _MAP = {
            'Description':   'description',
            'SampleFile':    'sample_file',
            'DataType':      'data_type',
            'CharSet':       'char_set',
            'PageBreaks':    'page_breaks',
            'LineBreaks':    'line_breaks',
            'PageViewMode':  'page_view_mode',
            'Formdef':       'formdef',
            'InputResPath':  'input_res_path',
        }
        for line in text.splitlines():
            line = line.strip().rstrip(';')
            if not line:
                continue
            m = re.match(r"^([\w\s]+?):\s*'?([^']*)'?\s*$", line)
            if not m:
                continue
            key   = m.group(1).strip()
            value = m.group(2).strip()
            attr  = _MAP.get(key)
            if attr:
                setattr(config, attr, value)
        return config

    # ── Field Block ───────────────────────────────────────────────────────────

    def _parse_field_section(self, text: str) -> Dict[str, FieldRule]:
        """Parse the 'field' block into {name: FieldRule}."""
        fields: Dict[str, FieldRule] = {}

        # Flatten to a single line, then split on ';'
        normalized = ' '.join(l.strip() for l in text.splitlines() if l.strip())
        definitions = [d.strip() for d in normalized.split(';') if d.strip()]

        for defn in definitions:
            try:
                colon = defn.index(':')
                name  = defn[:colon].strip()
                attrs = defn[colon + 1:].strip()
                fields[name] = self._parse_field_attrs(name, attrs)
            except (ValueError, IndexError) as exc:
                logger.warning("Skipping malformed field definition: %s (%s)", defn[:60], exc)

        return fields

    def _parse_field_attrs(self, name: str, attrs: str) -> FieldRule:
        """Translate a comma-separated attribute string into a FieldRule."""
        rule = FieldRule(name=name)

        # level=N
        m = re.search(r'level=(\d+)', attrs)
        if m:
            rule.level = int(m.group(1))

        # date('FORMAT')  — must come before generic 'string' check
        m = re.search(r"date\('([^']+)'\)", attrs)
        if m:
            rule.data_type  = 'date'
            rule.date_format = m.group(1)
        else:
            m = re.search(r'string\((\d+)\)', attrs)
            if m:
                rule.data_type  = 'string'
                rule.max_length = int(m.group(1))
            elif re.search(r'\bstring\b', attrs):
                rule.data_type = 'string'

        # padlen(N, 'CHAR')
        m = re.search(r"padlen\((\d+),\s*'(\w+)'\)", attrs)
        if m:
            rule.pad_length = int(m.group(1))
            word = m.group(2).upper()
            rule.pad_char = ' ' if word == 'SPACE' else word[0]

        # Boolean flags
        rule.single_instance = bool(re.search(r'\bsingleinstance\b', attrs))
        rule.allow_blank      = bool(re.search(r'\ballowblank\b',     attrs))

        # rowcol(R, C)
        m = re.search(r'rowcol\((\d+),\s*(\d+)\)', attrs)
        if m:
            rule.row = int(m.group(1))
            rule.col = int(m.group(2))

        # follows(TYPE, fieldname)
        m = re.search(r'follows\((\w+),\s*(\w+)\)', attrs)
        if m:
            rule.follows_type  = m.group(1)
            rule.follows_field = m.group(2)

        # matches('VALUE')  → anchor field
        m = re.search(r"matches\('([^']*)'\)", attrs)
        if m:
            rule.matches   = m.group(1)
            rule.is_anchor = True

        # replacetext('VALUE')
        m = re.search(r"replacetext\('([^']*)'\)", attrs)
        if m:
            rule.replace_text = m.group(1)

        # metadata('KEY')
        m = re.search(r"metadata\('([^']*)'\)", attrs)
        if m:
            rule.metadata_key = m.group(1)

        # formatconversion(N)
        m = re.search(r'formatconversion\((\d+)\)', attrs)
        if m:
            rule.format_conversion = int(m.group(1))

        return rule

    # ── Index Block ───────────────────────────────────────────────────────────

    def _parse_index_section(self, text: str) -> Dict[str, IndexRule]:
        """Parse the 'index' block into {name: IndexRule}."""
        indexes: Dict[str, IndexRule] = {}

        normalized  = ' '.join(l.strip() for l in text.splitlines() if l.strip())
        definitions = [d.strip() for d in normalized.split(';') if d.strip()]

        for defn in definitions:
            try:
                colon = defn.index(':')
                name  = defn[:colon].strip()
                attrs = defn[colon + 1:].strip()
                indexes[name] = self._parse_index_attrs(name, attrs)
            except (ValueError, IndexError) as exc:
                logger.warning("Skipping malformed index definition: %s (%s)", defn[:60], exc)

        return indexes

    def _parse_index_attrs(self, name: str, attrs: str) -> IndexRule:
        """Translate an index attribute string into an IndexRule."""
        rule = IndexRule(name=name)

        # GroupUsage(N)
        m = re.search(r'GroupUsage\((\d+)\)', attrs)
        if m:
            rule.group_usage = int(m.group(1))

        # GroupPersistenceYes / GroupPersistenceNo
        rule.group_persistence = bool(re.search(r'GroupPersistenceYes', attrs))

        # Strip known keywords; what remains are field references
        clean = re.sub(r'GroupUsage\(\d+\)',             '', attrs)
        clean = re.sub(r'GroupPersistenceYes|GroupPersistenceNo', '', clean)
        clean = re.sub(r'\s+', ' ', clean).strip()

        rule.fields = [f.strip() for f in clean.split(',') if f.strip()]
        return rule
