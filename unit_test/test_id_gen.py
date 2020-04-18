from unittest import TestCase
from unittest.mock import patch, Mock, call
from pmgdbutil import new_id

class IdGenTestCase(TestCase):
    @patch('pmgdbutil.gen_salt')
    def test_gen_new_id(self, gen_salt):
        gen_salt.side_effect = lambda ln: 'X' * ln
        cur = Mock()
        cur.fetchone.return_value = False
        gen_id = new_id(cur, 'tab', 2)
        self.assertEqual(gen_id, 'XX')

        cur.execute.assert_called_once_with("SELECT 1 FROM tab WHERE id = %(id)s", dict(id='XX'))

    @patch('pmgdbutil.gen_salt')
    def test_gen_new_id_with_cpx_key(self, gen_salt):
        gen_salt.side_effect = lambda ln: 'X' * ln
        cur = Mock()
        cur.fetchone.return_value = False
        gen_id = new_id(cur, 'tab', 2, exid='ABC')
        self.assertEqual(gen_id, 'XX')

        cur.execute.assert_called_once_with("SELECT 1 FROM tab WHERE id = %(id)s AND exid = %(exid)s", dict(id='XX', exid='ABC'))

    @patch('pmgdbutil.gen_salt')
    def test_gen_new_id_with_cpx_keys(self, gen_salt):
        gen_salt.side_effect = lambda ln: 'X' * ln
        cur = Mock()
        cur.fetchone.return_value = False
        gen_id = new_id(cur, 'tab', 2, exid='AbC', whyid='Whhyy', zedid='Whos Zed')
        self.assertEqual(gen_id, 'XX')

        cur.execute.assert_called_once_with("SELECT 1 FROM tab WHERE id = %(id)s AND exid = %(exid)s AND whyid = %(whyid)s AND zedid = %(zedid)s",
                dict(id='XX', exid='AbC', whyid='Whhyy', zedid='Whos Zed'))

    @patch('pmgdbutil.gen_salt')
    def test_gen_new_id_when_exists(self, gen_salt):
        class Gs(object):
            def __init__(self):
                self.called = 0
            def __call__(self, *args):
                if args:
                    # called as gen_salt
                    self.called += 1
                    return ('Y' if self.called > 1 else 'X') * args[0]
                return self.called <= 1 # called as fetchone

        gs = Gs()

        gen_salt.side_effect = gs
        cur = Mock()
        cur.fetchone.side_effect = gs
        gen_id = new_id(cur, 'tab', 4)
        self.assertEqual(gen_id, 'YYYY')

        cur.execute.assert_has_calls([
                call("SELECT 1 FROM tab WHERE id = %(id)s", dict(id='XXXX')),
                call("SELECT 1 FROM tab WHERE id = %(id)s", dict(id='YYYY'))])



