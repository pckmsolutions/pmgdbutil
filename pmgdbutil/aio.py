from contextlib import contextmanager, asynccontextmanager
import types
from itertools import count
from collections import defaultdict
from functools import wraps
import logging

from .std import row_as_dict, and_build_and

logger = logging.getLogger(__name__)

try:
    import cPickle as pickle
except ImportError:
    import pickle


async def fetchall_dict(cur):
    return (row_as_dict(cur, row) for row in await cur.fetchall())

async def fetchone_dict(cur, else_return = None):
    row = await cur.fetchone()
    return row_as_dict(cur, row) if row else (else_return() if else_return else None)

async def response_collection(cur, collection_name, *, mappers=None, limit=None, offset=None):
    def nam(k):
        return f'{collection_name}{k}'
    c_key, v_key = nam('_columns'), nam('')
    val_dict = {c_key: [d[0] for d in cur.description], v_key: await cur.fetchall()}

    val_dict[nam(f'_count')] = len(val_dict[v_key])

    for arg in ['limit', 'offset']:
        arg_val = locals().get(arg)
        if arg_val != None:
            val_dict[nam(f'_{arg}')] = arg_val

    if not mappers:
        return val_dict

    async def idf(x):
        return x
    
    val_mappers = defaultdict(lambda : idf)

    for col, ind in [(col, ind) for (col, ind) in zip(val_dict[c_key], count()) if col in mappers]:
        mapper = mappers[col]
        if any(isinstance(mapper, t) for t in (tuple, list)):
            val_dict[c_key][ind] = mappers[col][0]
            val_mappers[ind] = mappers[col][1]
        else:
            val_mappers[ind] = mappers[col]

    mapped = []
    for row in val_dict[v_key]:
        cols = []
        for ind in range(len(val_dict[c_key])):
            cols.append(await val_mappers[ind](row[ind]))

        mapped.append(tuple(cols))

    val_dict[v_key] = mapped

    return val_dict


@asynccontextmanager
async def temp_table(cur, name, defin):
    try:
        await cur.execute(f'CREATE TEMPORARY TABLE {name} ({defin})')
        yield
    finally:
        await cur.execute(f'DROP TABLE {name}')

@asynccontextmanager
async def lock_tables(cur, *names, lock_type='write'):
    try:
        await cur.execute(f'LOCK TABLES {",".join(names)} {lock_type}')
        yield
    finally:
        await cur.execute('UNLOCK TABLES')

try:
    from werkzeug.security import gen_salt
    async def new_id(cur, table, id_len, **kwargs):
        gen_id = lambda: gen_salt(id_len)
        test_id = gen_id()
        ex = f'SELECT 1 FROM {table} WHERE id = %(id)s'
        if kwargs:
            ex += ' ' + and_build_and(*[f'{k} = %({k})s' for k in kwargs.keys()])
    
        async def id_exists():
            await cur.execute(ex, {**dict(id=test_id), **kwargs})

            return bool(await cur.fetchone())
    
        while await id_exists():
            test_id = gen_id()
    
        return test_id
except ImportError:
    pass

