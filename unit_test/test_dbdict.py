from unittest import TestCase
from pmgdbutil import DbDict, DbConnectionPool

class DbDictTestCast(TestCase):

    def tearDown(self):
        self._get_dict().hard_reset()

    def _get_dict(self, **kwargs):
        pool = DbConnectionPool(1, host='192.168.1.21', port=3306, database='test', user='admin', password='admin123')
        return DbDict(pool, **{**dict(tablename='testdict'), **kwargs})

    def test_acts_like_dict(self):
        dbdict = self._get_dict()
        dbdict['heloo'] = 'hi'
        self.assertEqual(dbdict['heloo'], 'hi')

    def test_updates(self):
        dbdict = self._get_dict()
        dbdict['heloo'] = 'hi'
        dbdict['heloo'] = 'hiya'
        self.assertEqual(dbdict['heloo'], 'hiya')

    def test_gets_default(self):
        dbdict = self._get_dict()
        self.assertEqual(dbdict.get('not set', 'strufff'), 'strufff')
        self.assertIsNone(dbdict.get('also not set'))

    def test_throws_keyerror(self):
        dbdict = self._get_dict()
        with self.assertRaises(KeyError):
            dbdict['not set']

    def test_deletes(self):
        dbdict = self._get_dict()
        dbdict['heloo'] = 'you will delete'
        self.assertEqual(dbdict['heloo'], 'you will delete')
        del dbdict['heloo']
        self.assertIsNone(dbdict.get('heloo'))

    def test_expire(self):
        dbdict = self._get_dict()
        dbdict['heloox'] = 'you will expire'
        self.assertEqual(dbdict['heloox'], 'you will expire')
        dbdict.expire('heloox', -1)
        self.assertIsNone(dbdict.get('heloox'))

    def test_expire_with_set(self):
        dbdict = self._get_dict()
        dbdict.set('heloox', 'you will expire', -1)
        self.assertIsNone(dbdict.get('heloox'))

    def dont_test_delete_oldest(self):
        ''' this test will not work without delays inserted'''
        dbdict = self._get_dict(max_len=1)
        # insert delay here breakpoint()
        dbdict['helo1'] = 'hi1'
        self.assertEqual(dbdict['helo1'], 'hi1')
        # insert delay here breakpoint()
        dbdict['helo2'] = 'hi2'
        self.assertEqual(dbdict['helo2'], 'hi2')
        self.assertIsNone(dbdict.get('helo1'))




