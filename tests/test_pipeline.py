"""
test_pipeline.py
────────────────
Unit & integration tests for the ECM PoC pipeline.

Run with:
    cd ecm-poc
    python -m pytest tests/ -v
or directly:
    python tests/test_pipeline.py
"""

import os
import sys
import json
import tempfile
import unittest

# ── Path bootstrap ─────────────────────────────────────────────────────────────
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, 'src'))

from policy_parser   import PolicyParser, PolicyConfig, FieldRule, IndexRule
from doc_processor   import DocumentProcessor, Page
from field_extractor import FieldExtractor
from index_builder   import IndexBuilder
from main            import ECMProcessor


# ── Shared fixtures ───────────────────────────────────────────────────────────

POLICY_PATH = os.path.join(ROOT, 'policy', 'sample.policy')
INPUT_PATH  = os.path.join(ROOT, 'input',  'sample.txt')

MINI_POLICY = """
policy
Description: 'Test';
DataType: Text;
CharSet: ASCII;
PageBreaks: AnsiCC;
LineBreaks: CRLF;
PageViewMode: TextOnly;

field
anchor:     level=1, string(3), singleinstance, matches('***'), rowcol(1, 2);
accountnum: level=1, string(12), padlen(12, 'SPACE'), singleinstance, allowblank, rowcol(1, 9),  follows(SAMELINE, anchor);
masterid:   level=1, string(4),  padlen(4,  'SPACE'), singleinstance, allowblank, rowcol(1, 5),  follows(SAMELINE, anchor);
begindate:  level=1, string(8),  singleinstance, allowblank, rowcol(1, 33), follows(SAMELINE, anchor);
enddate:    level=1, string(8),  singleinstance, allowblank, rowcol(1, 41), follows(SAMELINE, anchor);
DOCNAME:    level=1, string, replacetext('EBR Statements'), singleinstance;
FILE_NAME:  level=1, string, metadata('FILE_NAME');

index
ACCTNUM:   GroupUsage(3), GroupPersistenceYes, accountnum;
BANKIDACC: GroupUsage(3), GroupPersistenceYes, masterid, accountnum;
STMTEND:   GroupUsage(3), GroupPersistenceYes, enddate;
DOCNAME:   GroupUsage(3), DOCNAME;
FILENAME:  GroupUsage(3), FILE_NAME;
"""

# Two-page sample — CRLF endings simulated as LF for in-memory tests
SAMPLE_DOC = (
    "1***B001ACC000001234RECIP0000001202401012024013100001\r\n"
    " STATEMENT LINE 1\r\n"
    " STATEMENT LINE 2\r\n"
    "1***B002ACC000005678RECIP0000002202402012024022900002\r\n"
    " ANOTHER STATEMENT\r\n"
)


# ── PolicyParser tests ────────────────────────────────────────────────────────

class TestPolicyParser(unittest.TestCase):

    def setUp(self):
        self.parser = PolicyParser()
        self.config, self.fields, self.indexes = self.parser.parse(MINI_POLICY)

    # policy block
    def test_config_data_type(self):   self.assertEqual(self.config.data_type,   'Text')
    def test_config_char_set(self):    self.assertEqual(self.config.char_set,    'ASCII')
    def test_config_page_breaks(self): self.assertEqual(self.config.page_breaks, 'AnsiCC')
    def test_config_line_breaks(self): self.assertEqual(self.config.line_breaks, 'CRLF')

    # field block
    def test_fields_all_parsed(self):
        for name in ('anchor', 'accountnum', 'masterid', 'begindate', 'DOCNAME', 'FILE_NAME'):
            self.assertIn(name, self.fields)

    def test_anchor_is_anchor(self):
        self.assertTrue(self.fields['anchor'].is_anchor)
        self.assertEqual(self.fields['anchor'].matches, '***')

    def test_anchor_rowcol(self):
        a = self.fields['anchor']
        self.assertEqual(a.row, 1)
        self.assertEqual(a.col, 2)

    def test_accountnum_attributes(self):
        f = self.fields['accountnum']
        self.assertEqual(f.max_length,    12)
        self.assertEqual(f.pad_length,    12)
        self.assertEqual(f.pad_char,      ' ')
        self.assertTrue(f.single_instance)
        self.assertTrue(f.allow_blank)
        self.assertEqual(f.follows_type,  'SAMELINE')
        self.assertEqual(f.follows_field, 'anchor')
        self.assertEqual(f.col,           9)

    def test_docname_replacetext(self):
        self.assertEqual(self.fields['DOCNAME'].replace_text, 'EBR Statements')

    def test_filename_metadata(self):
        self.assertEqual(self.fields['FILE_NAME'].metadata_key, 'FILE_NAME')

    # index block
    def test_indexes_all_parsed(self):
        for name in ('ACCTNUM', 'BANKIDACC', 'STMTEND'):
            self.assertIn(name, self.indexes)

    def test_index_group_usage(self):
        self.assertEqual(self.indexes['ACCTNUM'].group_usage, 3)

    def test_index_persistence(self):
        self.assertTrue(self.indexes['ACCTNUM'].group_persistence)

    def test_composite_index_fields(self):
        fields = self.indexes['BANKIDACC'].fields
        self.assertIn('masterid',   fields)
        self.assertIn('accountnum', fields)

    @unittest.skipUnless(os.path.exists(POLICY_PATH), "sample.policy not found")
    def test_full_policy_file(self):
        cfg, flds, idxs = PolicyParser().parse_file(POLICY_PATH)
        self.assertIsInstance(cfg, PolicyConfig)
        self.assertGreater(len(flds), 0)
        self.assertGreater(len(idxs), 0)
        self.assertIn('anchor',     flds)
        self.assertIn('accountnum', flds)
        self.assertIn('ACRCPSTMT', idxs)


# ── DocumentProcessor tests ───────────────────────────────────────────────────

class TestDocumentProcessor(unittest.TestCase):

    def setUp(self):
        _, self.config, _ = PolicyParser().parse(MINI_POLICY), *([None]*2)
        cfg, _, _ = PolicyParser().parse(MINI_POLICY)
        self.proc = DocumentProcessor(cfg)

    def test_two_pages_detected(self):
        pages = self.proc.process_text(SAMPLE_DOC)
        self.assertEqual(len(pages), 2)

    def test_page_numbers(self):
        pages = self.proc.process_text(SAMPLE_DOC)
        self.assertEqual(pages[0].page_number, 1)
        self.assertEqual(pages[1].page_number, 2)

    def test_first_line_contains_anchor(self):
        pages = self.proc.process_text(SAMPLE_DOC)
        # col 2-4 = index 1-3 in raw line (col 1 is the AnsiCC char)
        self.assertEqual(pages[0].lines[0][1:4], '***')

    def test_content_lines_page1(self):
        pages = self.proc.process_text(SAMPLE_DOC)
        self.assertEqual(len(pages[0].lines), 3)   # header + 2 content

    def test_page_get_line_valid(self):
        page = Page(page_number=1, lines=['line_a', 'line_b', 'line_c'])
        self.assertEqual(page.get_line(1), 'line_a')
        self.assertEqual(page.get_line(3), 'line_c')

    def test_page_get_line_out_of_bounds(self):
        page = Page(page_number=1, lines=['only_line'])
        self.assertEqual(page.get_line(99), '')

    def test_no_ansi_cc_single_page(self):
        doc = " line one\r\n line two\r\n line three\r\n"
        pages = self.proc.process_text(doc)
        self.assertEqual(len(pages), 1)

    @unittest.skipUnless(os.path.exists(INPUT_PATH), "sample.txt not found")
    def test_sample_file_has_pages(self):
        pages = self.proc.process_file(INPUT_PATH)
        self.assertGreater(len(pages), 0)


# ── FieldExtractor tests ──────────────────────────────────────────────────────

class TestFieldExtractor(unittest.TestCase):

    def setUp(self):
        cfg, fields, _ = PolicyParser().parse(MINI_POLICY)
        self.extractor = FieldExtractor(cfg, fields)
        self.proc      = DocumentProcessor(cfg)
        # Write SAMPLE_DOC to a real temp file so metadata fields can be resolved
        self._tmp = tempfile.NamedTemporaryFile(
            suffix='.txt', delete=False, mode='w', encoding='ascii'
        )
        self._tmp.write(SAMPLE_DOC)
        self._tmp.close()
        self.tmp_path = self._tmp.name

    def tearDown(self):
        os.unlink(self.tmp_path)

    def _page(self, n: int) -> Page:
        return self.proc.process_text(SAMPLE_DOC)[n - 1]

    # Anchor
    def test_anchor_found(self):
        self.assertIsNotNone(self.extractor.extract_page(self._page(1), self.tmp_path))

    def test_no_anchor_returns_none(self):
        empty_page = Page(page_number=99, lines=[" no anchor here at all"])
        self.assertIsNone(self.extractor.extract_page(empty_page, self.tmp_path))

    # Positional extraction
    def test_accountnum_page1(self):
        r = self.extractor.extract_page(self._page(1), self.tmp_path)
        self.assertEqual(r['accountnum'].strip(), 'ACC000001234')

    def test_masterid_page1(self):
        r = self.extractor.extract_page(self._page(1), self.tmp_path)
        self.assertEqual(r['masterid'].strip(), 'B001')

    def test_begindate_page1(self):
        r = self.extractor.extract_page(self._page(1), self.tmp_path)
        self.assertEqual(r['begindate'], '20240101')

    def test_enddate_page1(self):
        r = self.extractor.extract_page(self._page(1), self.tmp_path)
        self.assertEqual(r['enddate'], '20240131')

    def test_accountnum_page2(self):
        r = self.extractor.extract_page(self._page(2), self.tmp_path)
        self.assertEqual(r['accountnum'].strip(), 'ACC000005678')

    # replacetext
    def test_docname_hardcoded(self):
        r = self.extractor.extract_page(self._page(1), self.tmp_path)
        self.assertEqual(r['DOCNAME'], 'EBR Statements')

    # metadata
    def test_filename_metadata(self):
        r = self.extractor.extract_page(self._page(1), self.tmp_path)
        self.assertEqual(r['FILE_NAME'], os.path.basename(self.tmp_path))


# ── IndexBuilder tests ────────────────────────────────────────────────────────

class TestIndexBuilder(unittest.TestCase):

    def setUp(self):
        _, _, indexes = PolicyParser().parse(MINI_POLICY)
        self.builder = IndexBuilder(indexes, [])

    def test_single_field_index(self):
        idxs = self.builder.build_indexes({'accountnum': 'ACC000001234'})
        self.assertEqual(idxs['ACCTNUM'], 'ACC000001234')

    def test_composite_index(self):
        idxs = self.builder.build_indexes({'masterid': 'B001', 'accountnum': 'ACC000001234'})
        val  = idxs['BANKIDACC']
        self.assertIn('B001',         val)
        self.assertIn('ACC000001234', val)

    def test_persistence_across_pages(self):
        self.builder.build_indexes({'accountnum': 'ACC000001234'})
        idxs2 = self.builder.build_indexes({})           # no fields on page 2
        self.assertEqual(idxs2.get('ACCTNUM', ''), 'ACC000001234')

    def test_reset_clears_persistence(self):
        self.builder.build_indexes({'accountnum': 'ACC000001234'})
        self.builder.reset_persistence()
        idxs = self.builder.build_indexes({})
        self.assertEqual(idxs.get('ACCTNUM', ''), '')

    def test_docname_index(self):
        idxs = self.builder.build_indexes({'DOCNAME': 'EBR Statements'})
        self.assertEqual(idxs.get('DOCNAME', ''), 'EBR Statements')


# ── End-to-end integration tests ──────────────────────────────────────────────

class TestEndToEnd(unittest.TestCase):

    @unittest.skipUnless(
        os.path.exists(POLICY_PATH) and os.path.exists(INPUT_PATH),
        "sample.policy or sample.txt not found"
    )
    def test_full_pipeline_structure(self):
        proc   = ECMProcessor(POLICY_PATH)
        result = proc.process_file(INPUT_PATH)

        self.assertIn('file',          result)
        self.assertIn('total_pages',   result)
        self.assertIn('matched_pages', result)
        self.assertIn('pages',         result)
        self.assertEqual(result['total_pages'], result['matched_pages'])
        self.assertGreater(result['matched_pages'], 0)

    @unittest.skipUnless(
        os.path.exists(POLICY_PATH) and os.path.exists(INPUT_PATH),
        "sample.policy or sample.txt not found"
    )
    def test_extracted_fields(self):
        proc   = ECMProcessor(POLICY_PATH)
        result = proc.process_file(INPUT_PATH)
        fields = result['pages'][0]['extracted_fields']

        self.assertIn('accountnum', fields)
        self.assertIn('masterid',   fields)
        self.assertIn('begindate',  fields)
        self.assertIn('enddate',    fields)
        self.assertEqual(fields.get('DOCNAME'), 'EBR Statements')

    @unittest.skipUnless(
        os.path.exists(POLICY_PATH) and os.path.exists(INPUT_PATH),
        "sample.policy or sample.txt not found"
    )
    def test_indexes_built(self):
        proc    = ECMProcessor(POLICY_PATH)
        result  = proc.process_file(INPUT_PATH)
        indexes = result['pages'][0]['indexes']

        self.assertIn('ACCTNUM',   indexes)
        self.assertIn('BANKIDACC', indexes)
        self.assertIn('STMTEND',   indexes)
        self.assertIn('FILENAME',  indexes)

    @unittest.skipUnless(
        os.path.exists(POLICY_PATH) and os.path.exists(INPUT_PATH),
        "sample.policy or sample.txt not found"
    )
    def test_json_serialisable(self):
        proc   = ECMProcessor(POLICY_PATH)
        result = proc.process_file(INPUT_PATH)
        dumped = json.dumps(result)           # must not raise
        loaded = json.loads(dumped)
        self.assertEqual(len(loaded['pages']), result['matched_pages'])

    @unittest.skipUnless(
        os.path.exists(POLICY_PATH) and os.path.exists(INPUT_PATH),
        "sample.policy or sample.txt not found"
    )
    def test_four_pages_all_matched(self):
        proc   = ECMProcessor(POLICY_PATH)
        result = proc.process_file(INPUT_PATH)
        self.assertEqual(result['total_pages'],   4)
        self.assertEqual(result['matched_pages'], 4)


# ── Runner ────────────────────────────────────────────────────────────────────

if __name__ == '__main__':
    unittest.main(verbosity=2)
