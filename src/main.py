"""
main.py
───────
ECM PoC — end-to-end orchestrator.

Usage examples
──────────────
Single file:
    python src/main.py --policy policy/sample.policy ^
                       --input  input/sample.txt     ^
                       --output output/results.json

Directory:
    python src/main.py --policy policy/sample.policy ^
                       --input-dir input/            ^
                       --pattern "*.txt"             ^
                       --output output/results.json  ^
                       --verbose
"""

import os
import sys
import glob
import json
import logging
import argparse
from datetime import datetime
from typing import Any, Dict, List, Optional

# ── Path bootstrap (run from any directory) ───────────────────────────────────
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from policy_parser   import PolicyParser
from doc_processor   import DocumentProcessor, Page
from field_extractor import FieldExtractor
from index_builder   import IndexBuilder

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s  [%(levelname)-8s]  %(name)s — %(message)s',
    datefmt='%H:%M:%S',
)
logger = logging.getLogger('ecm_poc')


# ── ECMProcessor ──────────────────────────────────────────────────────────────

class ECMProcessor:
    """
    Top-level orchestrator.

    Lifecycle:
        processor = ECMProcessor('policy/sample.policy')
        result    = processor.process_file('input/sample.txt')
    """

    def __init__(self, policy_path: str):
        logger.info("Loading policy: %s", policy_path)
        parser = PolicyParser()
        self.config, self.field_rules, self.index_rules = parser.parse_file(policy_path)

        self.doc_processor   = DocumentProcessor(self.config)
        self.field_extractor = FieldExtractor(self.config, self.field_rules)
        self.index_builder   = IndexBuilder(self.index_rules, list(self.field_rules.keys()))

        logger.info(
            "Policy ready — %d field(s), %d index(es)",
            len(self.field_rules), len(self.index_rules),
        )

    # ── File processing ───────────────────────────────────────────────────────

    def process_file(self, file_path: str) -> Dict[str, Any]:
        """
        Process one document file end-to-end.

        Returns:
        {
          "file":          <absolute path>,
          "processed_at":  <ISO timestamp>,
          "total_pages":   <int>,
          "matched_pages": <int>,
          "pages": [
            {
              "page_number":       <int>,
              "extracted_fields":  { field_name: value, … },
              "indexes":           { index_name: value, … }
            }, …
          ]
        }
        """
        logger.info("Processing: %s", file_path)

        # Reset persistence for each new document
        self.index_builder.reset_persistence()

        pages = self.doc_processor.process_file(file_path)
        logger.info("  %d page(s) detected", len(pages))

        page_results: List[Dict] = []
        for page in pages:
            result = self._process_page(page, file_path)
            if result is not None:
                page_results.append(result)

        doc_result = {
            'file':          os.path.abspath(file_path),
            'processed_at':  datetime.now().isoformat(timespec='seconds'),
            'total_pages':   len(pages),
            'matched_pages': len(page_results),
            'pages':         page_results,
        }

        logger.info("  Done — %d/%d page(s) matched", len(page_results), len(pages))
        return doc_result

    def process_directory(
        self,
        dir_path: str,
        pattern:  str = '*.txt',
    ) -> List[Dict[str, Any]]:
        """Process all files in *dir_path* matching *pattern*."""
        files = sorted(glob.glob(os.path.join(dir_path, pattern)))
        if not files:
            logger.warning("No files found in '%s' matching '%s'", dir_path, pattern)
        results = []
        for fp in files:
            results.append(self.process_file(fp))
        return results

    # ── Page processing ───────────────────────────────────────────────────────

    def _process_page(
        self,
        page:      Page,
        file_path: str,
    ) -> Optional[Dict[str, Any]]:
        """Extract fields and build indexes for one page."""
        extracted = self.field_extractor.extract_page(page, file_path)
        if extracted is None:
            return None

        indexes = self.index_builder.build_indexes(extracted)

        return {
            'page_number':      page.page_number,
            'extracted_fields': extracted,
            'indexes':          indexes,
        }


# ── CLI ───────────────────────────────────────────────────────────────────────

def _build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description='ECM PoC — metadata extraction using Policy files',
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument('--policy',  required=True, help='Path to .policy file')

    group = p.add_mutually_exclusive_group(required=True)
    group.add_argument('--input',     help='Single input document path')
    group.add_argument('--input-dir', help='Directory containing input documents')

    p.add_argument('--output',  default='output/results.json',
                   help='Output JSON path (default: output/results.json)')
    p.add_argument('--pattern', default='*.txt',
                   help='File glob for --input-dir (default: *.txt)')
    p.add_argument('--verbose', '-v', action='store_true',
                   help='Enable DEBUG logging')
    return p


def _print_summary(results: List[Dict]) -> None:
    SEP = '=' * 65
    print(f'\n{SEP}')
    print('  ECM PoC - Extraction Summary')
    print(SEP)

    for doc in results:
        fname = os.path.basename(doc['file'])
        print(f'\n  File : {fname}')
        print(f'  Pages: {doc["matched_pages"]} matched / {doc["total_pages"]} total')

        for page in doc['pages']:
            print(f'\n  +-- Page {page["page_number"]} ' + '-' * 46)
            print('  |  Extracted Fields:')
            for k, v in page['extracted_fields'].items():
                if k == 'anchor':
                    continue
                print(f'  |    {k:<30}  =  {repr(v)}')
            print('  |  Indexes:')
            for k, v in page['indexes'].items():
                print(f'  |    {k:<20}  =  {repr(v)}')
            print('  +' + '-' * 55)

    print(f'\n{SEP}\n')


# ── Entry point ───────────────────────────────────────────────────────────────

def main() -> None:
    args = _build_arg_parser().parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    processor = ECMProcessor(args.policy)

    if args.input:
        results = [processor.process_file(args.input)]
    else:
        results = processor.process_directory(args.input_dir, args.pattern)

    out_dir = os.path.dirname(os.path.abspath(args.output))
    os.makedirs(out_dir, exist_ok=True)

    with open(args.output, 'w', encoding='utf-8') as fh:
        json.dump(results, fh, indent=2, default=str)

    logger.info("Results written → %s", args.output)
    _print_summary(results)


if __name__ == '__main__':
    main()
