from unittest import TestCase

from pmgdbutil import build_and, build_or, and_build_and, build_where, as_bool

class QueryFnTestCase(TestCase):
    def test_build_and(self):
        self.assertEqual(build_and(), '')
        self.assertEqual(build_and('a=b'), 'a=b')
        self.assertEqual(build_and('a=b', 'b=c'), 'a=b AND b=c')
        self.assertEqual(build_and('a=b', 'b=c', 'd=e'), 'a=b AND b=c AND d=e')
        self.assertEqual(build_and('', 'b=c'), 'b=c')
        self.assertEqual(build_and(None, 'b=c'), 'b=c')
        self.assertEqual(build_and('a=b', 'b=c', 'd=e', brackets=True), '(a=b AND b=c AND d=e)')
        self.assertEqual(build_and('a=b', 'b=c', 'd=e', brackets=False), 'a=b AND b=c AND d=e')
        self.assertEqual(build_and(brackets=True), '')

    def test_build_or(self):
        self.assertEqual(build_or(), '')
        self.assertEqual(build_or('a=b'), 'a=b')
        self.assertEqual(build_or('a=b', 'b=c'), 'a=b OR b=c')
        self.assertEqual(build_or('a=b', 'b=c', 'd=e'), 'a=b OR b=c OR d=e')
        self.assertEqual(build_or('', 'b=c'), 'b=c')
        self.assertEqual(build_or(None, 'b=c'), 'b=c')
        self.assertEqual(build_or('a=b', 'b=c', 'd=e', brackets=True), '(a=b OR b=c OR d=e)')
        self.assertEqual(build_or('a=b', 'b=c', 'd=e', brackets=False), 'a=b OR b=c OR d=e')
        self.assertEqual(build_or(brackets=True), '')

    def test_and_build_and(self):
        self.assertEqual(and_build_and(), '')
        self.assertEqual(and_build_and('a=b'), 'AND a=b')
        self.assertEqual(and_build_and('a=b', 'b=c'), 'AND a=b AND b=c')
        self.assertEqual(and_build_and('a=b', 'b=c', 'd=e'), 'AND a=b AND b=c AND d=e')
        self.assertEqual(and_build_and('', 'b=c'), 'AND b=c')
        self.assertEqual(and_build_and(None, 'b=c'), 'AND b=c')

    def test_build_where(self):
        self.assertEqual(build_where(), '')
        self.assertEqual(build_where('a=b'), 'WHERE a=b')
        self.assertEqual(build_where('a=b', 'b=c'), 'WHERE a=b AND b=c')
        self.assertEqual(build_where('a=b', 'b=c', 'd=e'), 'WHERE a=b AND b=c AND d=e')
        self.assertEqual(build_where('a=b', '  ', 'b=c'), 'WHERE a=b AND b=c')

    def test_as_bool(self):
        self.assertFalse(as_bool(None))
        self.assertFalse(as_bool('text'))
        self.assertFalse(as_bool('1zero'))
        self.assertFalse(as_bool('zero1'))
        self.assertFalse(as_bool('0'))
        self.assertTrue(as_bool('1'))
        self.assertTrue(as_bool('100001'))




