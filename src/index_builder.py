"""
index_builder.py
────────────────
Builds named search indexes from extracted field values using IndexRule
definitions produced by PolicyParser.

Key behaviours:
  GroupPersistenceYes  — a field value, once seen, is remembered and
                         reused on subsequent pages of the SAME document
                         even if the field is not re-extracted on that page.
                         Call reset_persistence() between documents.

  Composite indexes    — multiple field references are joined with '|' as
                         the separator (easy to split in downstream code).

  Name resolution      — index field refs may use spaces where the field
                         block uses underscores (e.g. "FILE DIR1" vs
                         "FILE_DIR1").  Both forms are tried automatically.
"""

import logging
from typing import Dict, List, Optional

from policy_parser import IndexRule

logger = logging.getLogger(__name__)


class IndexBuilder:
    """Build index values from a page's extracted fields."""

    _SEP = '|'   # separator between composite-index components

    def __init__(
        self,
        index_rules: Dict[str, IndexRule],
        field_names: List[str],
    ):
        self.index_rules = index_rules
        self.field_names = field_names
        self._persist:  Dict[str, str] = {}   # persistence cache: field → value

    # ── Public API ────────────────────────────────────────────────────────────

    def build_indexes(self, extracted_fields: Dict[str, str]) -> Dict[str, str]:
        """
        Build all configured indexes for one page.

        Args:
            extracted_fields: {field_name: value} from FieldExtractor.

        Returns:
            {index_name: composite_value}
        """
        self._refresh_cache(extracted_fields)

        indexes: Dict[str, str] = {}
        for idx_name, rule in self.index_rules.items():
            value = self._build_one(rule, extracted_fields)
            if value is not None:
                indexes[idx_name] = value
                logger.debug("Index %-20s = '%s'", idx_name, value)

        return indexes

    def reset_persistence(self) -> None:
        """
        Clear the persistence cache.
        Must be called at the start of each new document so values from
        one document do not bleed into the next.
        """
        self._persist.clear()
        logger.debug("Persistence cache cleared.")

    # ── Internal ──────────────────────────────────────────────────────────────

    def _refresh_cache(self, extracted: Dict[str, str]) -> None:
        """Store every non-blank extracted value in the persistence cache."""
        for name, value in extracted.items():
            if value and value.strip():
                self._persist[name] = value

    def _build_one(
        self,
        rule:       IndexRule,
        extracted:  Dict[str, str],
    ) -> Optional[str]:
        """Build a single index value from its field references."""
        if not rule.fields:
            return None

        parts: List[str] = []
        for field_ref in rule.fields:
            v = self._resolve(field_ref, extracted, rule.group_persistence)
            parts.append(v.strip())

        return self._SEP.join(parts)

    def _resolve(
        self,
        field_ref:       str,
        extracted:       Dict[str, str],
        use_persistence: bool,
    ) -> str:
        """
        Resolve one field reference to its value.

        Lookup order:
          1. Exact match in extracted fields.
          2. Name with spaces ↔ underscores swapped.
          3. Exact match in persistence cache (if GroupPersistenceYes).
          4. Alternate name in persistence cache.
          5. Empty string.
        """
        alt_ref = (field_ref.replace(' ', '_')
                   if ' ' in field_ref
                   else field_ref.replace('_', ' '))

        candidates = [field_ref, alt_ref]

        # Check live extracted values first
        for name in candidates:
            if name in extracted:
                v = extracted[name]
                if v or not use_persistence:
                    return v

        # Fall back to persistence cache
        if use_persistence:
            for name in candidates:
                if name in self._persist:
                    return self._persist[name]

        return ''
