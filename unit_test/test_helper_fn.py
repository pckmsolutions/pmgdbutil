
from unittest import TestCase

from pmgdbutil import str_to_time, response_collection

class HelperFnTestCase(TestCase):
    def test_time_cvt(self):
        self.assertEqual(str_to_time('0:0:0'), 0)
        self.assertEqual(str_to_time('0:0:5'), 5)
        self.assertEqual(str_to_time('0:10:0'), 10*60)
        self.assertEqual(str_to_time('33:20:10'), 33*60*60+20*60+10)
        self.assertEqual(str_to_time('10:08'), 10*60+8)
        self.assertEqual(str_to_time('1000:08'), 1000*60+8)
        with self.assertRaises(ValueError):
            str_to_time('unknown')
    class Cur(object):
        def __init__(self, cols, vals):
            self.description = cols
            self.vals = vals

        def fetchall(self):
            return self.vals

    def test_response_collection(self):
        cur = HelperFnTestCase.Cur([('col1',), ('col2',), ('col3',)], [('val11', 'val12', 'val13'), ('val21', 'val22', 'val23')])

        val_dict = response_collection(cur, 'mystuf')
        self.assertEqual(val_dict,
                {'mystuf_columns': ['col1', 'col2', 'col3'],
                    'mystuf':[('val11', 'val12', 'val13'), ('val21', 'val22', 'val23')]})

    def test_response_collection(self):
        cur = HelperFnTestCase.Cur([('col1',), ('col2',), ('col3',)], [('val11', 'val12', 'val13'), ('val21', 'val22', 'val23')])

        val_dict = response_collection(cur, 'mystuf', col2=('colx', lambda v: v * 2))
        self.assertEqual(val_dict,
                {'mystuf_columns': ['col1', 'colx', 'col3'],
                    'mystuf':[('val11', 'val12val12', 'val13'), ('val21', 'val22val22', 'val23')]})

    def test_response_collection_with_numbers_coz_this_is_awesome(self):
        cur = HelperFnTestCase.Cur([('col1',), ('col2',), ('col3',)], [(11, 12, 13), (21, 22, 23)])

        val_dict = response_collection(cur, 'mystuf', col1=('colA', lambda v: v + 5), col2=('colB', lambda v: v * 2), col3=('colC', lambda v: v - 11))
        self.assertEqual(val_dict,
                {'mystuf_columns': ['colA', 'colB', 'colC'],
                    'mystuf':[(16, 24, 2), (26, 44, 12)]})

