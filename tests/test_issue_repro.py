import unittest
import os
from lineage.extractor import LineageExtractor
from lineage.logger import get_logger

class TestIssueRepro(unittest.TestCase):
    def setUp(self):
        self.logger = get_logger()
        self.extractor = LineageExtractor(engine="spark", logger=self.logger)
        self.sql_file = "temp_test_issue.sql"

    def tearDown(self):
        if os.path.exists(self.sql_file):
            os.remove(self.sql_file)

    def test_validation_condition_tracking(self):
        # SQL with the fix applied (aliasing dr_cr as debit_credit)
        sql = """
        with temp_init(
            select b.dr_cr as debit_credit,cif
            from (select cif from db.test1) as a
            inner join (select dr_cr from db.test3) as b on a.cif = b.cif
        )
        , temp_oth(
            select sum(case when debit_credit = 'D' then 1 else 0 end) as cnt_debit
            ,cif
            from temp_init
        )
        ,perc_temp(
            select a.*,2.5*col1 as col2, 
            from (
                select cif,avg(cnt_debit) as col1 from temp_oth
            ) a
        ),
        final_temp(
            select case when col2 > 10 then cif else 0 end as col3
            from perc_temp
        )
        select col3 from final_temp
        """
        
        with open(self.sql_file, "w") as f:
            f.write(sql)
            
        results = self.extractor.extract_from_file(self.sql_file)
        lineage = results['lineage']
        
        # Verify we have two rows
        self.assertEqual(len(lineage), 2, f"Expected 2 lineage rows, got {len(lineage)}")
        
        # Verify sources
        sources = {(r.source_table, r.source_column) for r in lineage}
        expected_sources = {('db.test1', 'cif'), ('db.test3', 'dr_cr')}
        
        self.assertEqual(sources, expected_sources, f"Expected sources {expected_sources}, got {sources}")
        
        # Verify target column
        for r in lineage:
            self.assertEqual(r.target_column, 'col3')

if __name__ == "__main__":
    unittest.main()
