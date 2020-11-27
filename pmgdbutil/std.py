from contextlib import contextmanager
import types
from itertools import count
from collections import defaultdict, namedtuple
from functools import wraps
import logging

logger = logging.getLogger(__name__)

try:
    import cPickle as pickle
except ImportError:
    import pickle

def row_as_dict(cur, row):
    return {col_desc[0]: val for (col_desc, val) in zip(cur.description, row)} 

def fetchall_dict(cur):
    return (row_as_dict(cur, row) for row in cur.fetchall())

def fetchone_dict(cur, else_return = None):
    row = cur.fetchone()
    return row_as_dict(cur, row) if row else (else_return() if else_return else None)

def fetchone_tuple(cur):
    One = namedtuple('One', ' '.join([col[0] for col in cur.description]))
    row = cur.fetchone()
    return One(*row) if row else None

def limit_offset(req_dict):
    def page_str(key):
        val = as_int(req_dict.get(key))
        if not val:
            return ''
        return f'{key.upper()} {int(val)}'
    return f'{page_str("limit")} {page_str("offset")}'

def search_match(req_dict, *fields):
    if req_dict.get('search'):
        return f'MATCH ({",".join(fields)}) AGAINST (%(search)s IN NATURAL LANGUAGE MODE)'
    return ''

def like_match(req_dict, *fields):
    if req_dict.get('like'):
        return build_or(*[f'{fld} LIKE %(like)s' for fld in fields])
    return ''

def lastdate_match(req_dict, field):
    return f"DATE({field}) <= %(lastdate)s" if req_dict.get('lastdate') else ''

def firstdate_match(req_dict, field):
    return f"DATE({field}) >= %(firstdate)s" if req_dict.get('firstdate') else ''

def build_bop(bop, *vargs, **kwargs):
    ret = f' {bop} '.join([m for m in (m.strip() for m in vargs if m) if m]) if vargs else ''
    return ret if not kwargs.get('brackets') or not ret else f'({ret})'

def build_and(*vargs, **kwargs):
    return build_bop('AND', *vargs, **kwargs)

def build_or(*vargs, **kwargs):
    return build_bop('OR', *vargs, **kwargs)

def and_build_and(*vargs):
    and_str = build_and(*vargs)
    return f'AND {and_str}' if and_str else ''

def build_where(*vargs):
    matches = build_and(*vargs)
    return f'WHERE {matches}' if matches else ''

def as_bool(given):
    try:
        return bool(int(given)) if given else False
    except (ValueError, TypeError):
        return False

def as_int(given):
    try:
        return int(given) if given else None
    except (ValueError, TypeError):
        return None

def query_args(source_dict, *args, **kwargs):
    check_list = list(args)
    if kwargs.get('search_args'):
        check_list += ['limit', 'offset', 'lastdate', 'firstdate', 'search', 'like']
    return {k:v for (k, v) in source_dict.items() if k in check_list}

def response_collection(cur, collection_name, *, mappers=None, limit=None, offset=None):
    def nam(k):
        return f'{collection_name}{k}'
    c_key, v_key = nam('_columns'), nam('')
    val_dict = {c_key: [d[0] for d in cur.description], v_key: cur.fetchall()}

    val_dict[nam(f'_count')] = len(val_dict[v_key])

    for arg in ['limit', 'offset']:
        arg_val = locals().get(arg)
        if arg_val != None:
            val_dict[nam(f'_{arg}')] = arg_val

    if not mappers:
        return val_dict

    val_mappers = defaultdict(lambda : lambda x:x)
    for col, ind in [(col, ind) for (col, ind) in zip(val_dict[c_key], count()) if col in mappers]:
        mapper = mappers[col]
        if any(isinstance(mapper, t) for t in (tuple, list)):
            val_dict[c_key][ind] = mappers[col][0]
            val_mappers[ind] = mappers[col][1]
        else:
            val_mappers[ind] = mappers[col]

    val_dict[v_key] = [tuple([val_mappers[ind](row[ind]) for ind in range(len(val_dict[c_key]))]) for row in val_dict[v_key]]
    return val_dict

@contextmanager
def temp_table(cur, name, defin):
    try:
        cur.execute(f'CREATE TEMPORARY TABLE {name} ({defin})')
        yield
    finally:
        cur.execute(f'DROP TABLE {name}')

@contextmanager
def lock_tables(cur, *names, lock_type='write'):
    try:
        cur.execute(f'LOCK TABLES {",".join(names)} {lock_type}')
        yield
    finally:
        cur.execute('UNLOCK TABLES')

try:
    from werkzeug.security import gen_salt
    def new_id(cur, table, id_len, **kwargs):
        gen_id = lambda: gen_salt(id_len)
        test_id = gen_id()
        ex = f'SELECT 1 FROM {table} WHERE id = %(id)s'
        if kwargs:
            ex += ' ' + and_build_and(*[f'{k} = %({k})s' for k in kwargs.keys()])
    
        def id_exists():
            cur.execute(ex, {**dict(id=test_id), **kwargs})

            return bool(cur.fetchone())
    
        while id_exists():
            test_id = gen_id()
    
        return test_id
except ImportError:
    pass

class DbDict(object):
    def __init__(self, connection_pool, **kwargs):
        self.tablename = kwargs['tablename']
        for k in [('threshold', None), ('max_age_seconds', None), ('max_key_size', 100), ('max_val_size', 500)]:
            setattr(self, k[0], kwargs.get(k[0], k[1]))
        self.connection_pool = connection_pool

        with self._cur() as cur:
            cur.execute(f'''
                CREATE TABLE IF NOT EXISTS {self.tablename} (
                   `key` VARCHAR({self.max_key_size}) NOT NULL,
                   val VARBINARY({self.max_val_size}),
                   expires INT,
                   touched DATETIME NOT NULL,
                   PRIMARY KEY (`key`)
                )
                ''')

    def hard_reset(self):
        with self._cur() as cur:
            cur.execute(f'DROP TABLE IF EXISTS {self.tablename}')

    def __setitem__(self, key, val):
        self.set(key, val)

    def set(self, key, val, expire_secs=None):
        with self._cur() as cur:
            cur.execute(f'''
                INSERT INTO {self.tablename} (`key`, val, expires, touched)
                VALUES(%(key)s, %(val)s, %(expires)s, NOW())
                ON DUPLICATE KEY UPDATE
                    val = %(val)s,
                    expires = %(expires)s,
                    touched = NOW()
            ''', dict(key=key, val=pickle.dumps(val), expires=expire_secs if expire_secs else self.max_age_seconds))

    def __getitem__(self, key):
        value = self.get(key)
        if not value:
            raise KeyError(key)
        return value

    def get(self, key, default_value=None):
        with self._cur() as cur:
            self._prune(cur)
            cur.execute(f'''
                SELECT val FROM {self.tablename} 
                WHERE `key` = %s
            ''', (key,))
            row = cur.fetchone()
            if not row:
                return default_value
            val = row[0]
            try:
                cur.execute(f'''
                    UPDATE {self.tablename} 
                    SET touched = NOW()
                    WHERE `key` = %s
                ''', (key,))
            except:
                pass

            return pickle.loads(val)

    def _prune(self, cur):
        cur.execute(f'DELETE FROM {self.tablename} WHERE DATE_ADD(touched, INTERVAL expires second) <= NOW()')
        if self.threshold:
            cur.execute(f'SELECT count(*) FROM {self.tablename}')
            count = int(cur.fetchone()[0])
            if count > self.threshold:
                session.execute(f'DELETE FROM {self.tablename} ORDER BY touched LIMIT %s', (count - self.threshold, ))

    def expire(self, key, insecs):
        with self._cur() as cur:
            cur.execute(f'''
                UPDATE {self.tablename} 
                SET expires = %s
                WHERE `key` = %s
            ''', (insecs, key,))

    def __delitem__(self, key):
        with self._cur() as cur:
            cur.execute(f'DELETE FROM {self.tablename} WHERE `key` = %s', (key,))

    @contextmanager
    def _cur(self):
        with self.connection_pool.get_connection() as connection:
            cur = connection.cursor(buffered=True)
            try:
                yield cur
                connection.commit()
            except:
                connection.rollback()
                raise
            finally:
                cur.close()

def last_id(cur):
    cur.execute('SELECT LAST_INSERT_ID()')
    return cur.fetchone()[0]

def str_to_time(strng):
    parts = strng.split(':')
    n,result = len(parts) - 1,int(float(parts[-1]))
    for p in parts[0:-1]:
        result += int(p)*pow(60,n)
        n -= 1
    return result

def opt_assign(data, update_name, exist_name):
    def val_assign_for_optional(key):
        return f'{update_name}.{key} = {exist_name}.{key}' if not key in data \
                else f'{update_name}.{key} = %({key})s'
    return val_assign_for_optional

@contextmanager
def connected_cursor(**kwargs):
    with connection(**kwargs) as cnx:
        cur = cnx.cursor()
        try:
            yield cur
        finally:
            cur.close()

def with_cursor(getcon, **kwargs):
    def wrapped(view_func):
        @wraps(view_func)
        def decorated(*args, **kwargs):
            with getcon() as connection:
                cur = connection.cursor(buffered=True)
                try:
                    ret = view_func(cur, *args, **kwargs)
                    connection.commit()
                    return ret
                except:
                    logger.error('Error in view function call. Rolling back database.')
                    connection.rollback()
                    raise
                finally:
                    if cur:
                        cur.close()

        return decorated
    return wrapped
