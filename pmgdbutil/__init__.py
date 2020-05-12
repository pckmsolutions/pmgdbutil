from contextlib import contextmanager
from mysql.connector import pooling, connect
from mysql.connector.connection import ServerCmd
import types
from itertools import count
from collections import defaultdict
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

def fetchone_obj(cur):
    class OneRow(object):
        def __init__(self, row):
            for (col_desc, val) in zip(cur.description, row):
                setattr(self, col_desc[0], val)
    row = cur.fetchone()
    return OneRow(row) if row else None

def limit_offset(req_dict):
    def page_str(key):
        val = as_int(req_dict.get(key))
        if not val:
            return ''
        return f'{key.upper()} {int(val)}'
    return f'{page_str("limit")} {page_str("offset")}'

def search_match(req_dict, *fields):
    search = req_dict.get('search')
    if search:
        return f'MATCH ({",".join(fields)}) AGAINST (%(search)s IN NATURAL LANGUAGE MODE)'
    return ''

def lastdate_match(req_dict, field):
    return f"{field} <= %(lastdate)s" if req_dict.get('lastdate') else ''

def firstdate_match(req_dict, field):
    return f"{field} >= %(firstdate)s" if req_dict.get('firstdate') else ''

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
    check_list = [*args, *['limit', 'offset', 'lastdate', 'firstdate', 'search']] if kwargs.get('search_args') else args
    return {k:v for (k, v) in source_dict.items() if k in check_list}

def response_collection(cur, collection_name, **mappers):
    c_key, v_key = f'{collection_name}_columns', f'{collection_name}'
    val_dict = {c_key: [d[0] for d in cur.description], v_key: cur.fetchall()}
    if not mappers:
        return val_dict

    val_mappers = defaultdict(lambda : lambda x:x)
    for col, ind in [(col, ind) for (col, ind) in zip(val_dict[c_key], count()) if col in mappers]:
        val_dict[c_key][ind] = mappers[col][0]
        val_mappers[ind] = mappers[col][1]

    val_dict[v_key] = [tuple([val_mappers[ind](row[ind]) for ind in range(len(val_dict[c_key]))]) for row in val_dict[v_key]]
    return val_dict


class DbConnectionPool(object):
    def __init__(self, pool_size, **kwargs):
        self.pool = pooling.MySQLConnectionPool(pool_size=pool_size, **kwargs) 

    @contextmanager
    def get_connection(self):
        cnx = self.pool.get_connection()
        try:
            def _cmd_reset_connection(self):
                self._handle_ok(self._send_cmd(ServerCmd.RESET_CONNECTION))
                self._post_connection()

            cnx._cnx.cmd_reset_connection = types.MethodType(_cmd_reset_connection, cnx)
            cnx.autocommit = False
            yield cnx
        finally:
            cnx.close()

    def close(self):
        self.pool._remove_connections()
        self.pool = None

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
def connection(**kwargs):
    cnx = connect(**kwargs)
    cnx.autocommit = False
    try:
        yield cnx
    finally:
        cnx.commit()
        cnx.close()

@contextmanager
def connected_cursor(**kwargs):
    with connection(**kwargs) as cnx:
        cur = cnx.cursor()
        try:
            yield cur
        finally:
            cur.close()

class ConnectedCursor(object):
    def __init__(self, connection_args, cursor_args=None):
        self.cnx_args = connection_args
        self.cur_args = cursor_args or {}
        self.cnx = None
        self.cur = None

    def close(self, commit=True):
        if self.cur:
            self.cur.close()
            self.cur = None
        if self.cnx:
            if commit:
                self.cnx.commit()
            else:
                self.cnx.rollback()
            self.cnx.close()
            self.cnx = None

    def __enter__(self):
        self.cnx = connect(**self.cnx_args)
        self.cnx.autocommit = False
        self.cur = self.cnx.cursor(**self.cur_args)
        return self

    def __exit__(self ,type, value, traceback):
        self.close(type == None)

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



