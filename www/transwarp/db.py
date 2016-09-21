#!/usr/bin/env python
# -*- encoding: utf-8 -*-

"""
db模块
2016_9_20
"""

import threading
import functools
import time

# 全局变量 数据库连接
engine = None

def create_engine(user, passwd, db, host='127.0.0.1', port=3306, **kw):
    """
    创建数据库连接
    """
    import MySQLdb
    global engine
    if engine is not None:
        raise DBError('Engine is already initialized.')
    params = dict(user=user, passwd=passwd, db=db, host=host, port=port)
    defaults = dict(use_unicode=True, charset='utf8')
    for k, v in defaults.iteritems():
        params[k] = kw.pop(k, v)
    engine = _Engine(lambda: MySQLdb.connect(**params))

def connection():
    """
    获取数据库的连接
    """
    return _ConnectionCtx()

def with_connection(func):
    """
    设计一个装饰器替换with语法
    """
    @functools.wraps(func)
    def _wrapper(*args, **kw):
        with _ConnectionCtx():
            return func(*args, **kw)
    return _wrapper

def transaction():
    """
    获取数据库事务功能
    """
    return _TransactionCtx()

def with_transaction(func):
    @functools.wraps(func)
    def _wrapper(*args, **kw):
        with _TransactionCtx():
            func(*args, **kw)
    return _wrapper


# 自定义错误类型
class DBError(Exception):
    pass


class _Engine(object):
    """
    数据库引擎对象
    """
    def __init__(self, connect):
        self._connect = connect
    def connect(self):
        return self._connect()

class _LasyConnection(object):
    """
    惰性连接对象，仅当需要cursor对象才连接数据库，获取连接
    """
    def __init__(self):
        self.connection = None

    def cursor(self):
        if self.connection is None:
            _connection = engine.connect()
            self.connection = _connection
        return self.connection.cursor()

    def commit(self):
        self.connection.commit()

    def rollback(self):
        self.connection.rollback()

    def cleanup(self):
        if self.connection:
            _connection = self.connection
            self.connection = None
            _connection.close()

class _DbCtx(threading.local):
    """
    持有数据库连接的上下文对象
    """
    def __init__(self):
        self.connection = None
        self.transactions = 0

    def is_init(self):
        return not self.connection is None

    def init(self):
        self.connection = _LasyConnection()
        self.transactions = 0

    def cleanup(self):
        self.connection.cleanup()
        self.connection = None

    def cursor(self):
        return self.connection.cursor()

# 全局变量 存放每个线程的数据库连接（_DbCtx继承自ThreadLocal）
_db_ctx = _DbCtx()

class _ConnectionCtx(object):
    """
    数据库连接上下文，实现自动获取和释放连接
    """
    def __enter__(self):
        global _db_ctx
        self.should_cleanup = False
        if not _db_ctx.is_init():
            _db_ctx.init()
            self.should_cleanup = True
        return self

    def __exit__(self, exctype, excvalue, traceback):
        global _db_ctx
        if self.should_cleanup:
            _db_ctx.cleanup()

class _TransactionCtx(object):
    """
    事务提交、回滚等
    """
    def __enter__(self):
        global _db_ctx
        self.should_close_conn = False
        if not _db_ctx.is_init():
            _db_ctx.init()
            self.should_close_conn = True
        _db_ctx.transactions = _db_ctx.transactions + 1
        return self

    def __exit__(self, exctype, excvalue, traceback):
        global _db_ctx
        _db_ctx.transactions = _db_ctx.transactions - 1
        try:
            if _db_ctx.transactions==0:
                if exctype is None:
                    self.commit()
                else:
                    self.rollback()
        finally:
            if self.should_close_conn:
                _db_ctx.cleanup()

    def commit(self):
        global _db_ctx
        try:
            _db_ctx.connection.commit()
        except :
            _db_ctx.connection.rollback()
            raise

    def rollback(self):
        global _db_ctx
        _db_ctx.connection.rollback()

class Dict(dict):
    """
    字典对象，简化访问。如 x.key = value
    """
    def __init__(self, names=(), values=(), **kw):
        super(Dict, self).__init__(**kw)
        for k, v in zip(names, values):
            self[k] = v

    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(r"'Dict' object has no attribute '%s'" % key)

    def __setattr__(self, key, value):
        self[key] = value 

@with_connection
def select(sql, *args):
    """
    执行select语句，返回多个结果组成的列表
    """
    global _db_ctx
    cursor = None
    sql = sql.replace('?', '%s')
    try:
        cursor = _db_ctx.connection.cursor()
        cursor.execute(sql, args)
        if cursor.description:
            names = [x[0] for x in cursor.description]
        return [Dict(names, x) for x in cursor.fetchall()]
    finally:
        if cursor:
            cursor.close()

@with_connection
def update(sql, *args):
    """
    执行update语句，返回影响的行数
    """
    global _db_ctx
    cursor = None
    sql = sql.replace('?', '%s')
    try:
        cursor = _db_ctx.connection.cursor()
        cursor.execute(sql, args)
        r = cursor.rowcount
        if _db_ctx.transactions == 0:
            _db_ctx.connection.commit()
        return r
    finally:
        if cursor:
            cursor.close()


if __name__=='__main__':
    create_engine('www-data', 'www-data', 'test')
    
    # 建表
    # update('drop table if exists test')
    # update('create table test (id int primary key, name text, email text, passwd text, last_modified real)')
    
    # 插入数据
    # # u1 = dict(id=2000, name='Bob', email='bob@test.org', passwd='bobobob', last_modified=time.time())
    # u1 = dict(id=2001, name='Zzh', email='zzh@test.org', passwd='zzzzzzz', last_modified=time.time())
    # keys = [key for key in u1]
    # value = [u1[key] for key in keys]
    # sql = 'insert into test('+keys[0]+','+keys[1]+','+keys[2]+','+keys[3]+','+keys[4]+') values(?,?,?,?,?)'
    # update(sql, *value)

    # 查找数据
    sql = 'select * from test'
    print select(sql)
