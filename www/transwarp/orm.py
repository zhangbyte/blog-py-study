#!/usr/bin/env python
# -*- encoding: utf-8 -*-

"""
orm模块
2016_9_21
"""

import db

class Field(object):

    def __init__(self, **kw):
        self.name = kw.get('name', None)
        self._default = kw.get('default', None)
        self.primary_key = kw.get('primary_key', False)
        self.nullable = kw.get('nullable', False)
        self.updateable = kw.get('updateable', False)
        self.insertable = kw.get('insertable', False)
        self.ddl = kw.get('ddl', '')

    @property
    def default(self):
        d = self._default
        return d() if callable(d) else d

    def __str__(self):
        s = ['<%s:%s,%s,default(%s)' % (self.__class__.__name__, self.name, self.ddl, self._default)]
        self.nullable and s.append('N')
        self.updateable and s.append('U')
        self.insertable and s.append('I')
        s.append('>')
        return ''.join(s)

class StringField(Field):
    def __init__(self, **kw):
        if 'default' not in kw:
            kw['default'] = ''
        if 'ddl' not in kw:
            kw['ddl'] = 'varchar(255)'
        super(StringField, self).__init__(**kw)

class IntegerField(Field):
    def __init__(self, **kw):
        if 'default' not in kw:
            kw['default'] = 0
        if 'ddl' not in kw:
            kw['ddl'] = 'bigint'
        super(IntegerField, self).__init__(**kw)

class FloatField(Field):
    def __init__(self, **kw):
        if 'default' not in kw:
            kw['default'] = 0.0
        if 'ddl' not in kw:
            kw['ddl'] = 'real'
        super(FloatField, self).__init__(**kw)

class BooleanField(Field):
    def __init__(self, **kw):
        if 'default' not in kw:
            kw['default'] = False
        if 'ddl' not in kw:
            kw['ddl'] = 'bool'
        super(BooleanField, self).__init__(**kw)

class TextField(Field):
    def __init__(self, **kw):
        if 'default' not in kw:
            kw['default'] = ''
        if 'ddl' not in kw:
            kw['ddl'] = 'blob'
        super(TextField, self).__init__(**kw)

class VersionField(Field):
    def __init__(self, name=None):
        super(VersionField, self).__init__(name=name, default=0, ddl='bigint')

class ModelMetaclass(type):
    def __new__(cls, name, bases, attrs):
        if name=='Model':
            return type.__new__(cls, name, bases, attrs)

        mappings = dict()
        primary_key = None
        for k, v in attrs.iteritems():
            if isinstance(v, Field):
                if not v.name:
                    v.name = k
                print('Found mapping: %s==>%s' % (k, v))
                if v.primary_key:
                    if primary_key:
                        raise TypeError('Cannot define more than 1 primary key in class: %s' % name)
                    primary_key = v
                mappings[k] = v
        if not primary_key:
            raise TypeError('Primary key not defined in class: %s' % name)
        for k in mappings.iterkeys():
            attrs.pop(k)
        if not '__table__' in attrs:
            attrs['__table__'] = name.lower()
        attrs['__mappings__'] = mappings
        attrs['__primary_key__'] = primary_key
        return type.__new__(cls, name, bases, attrs)

class Model(dict):
    __metaclass__=ModelMetaclass

    def __init__(self, **kw):
        super(Model, self).__init__(**kw)

    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(r"'Dict' object has no attribute '%s'" % key)

    def __setattr__(self, key, value):
        self[key] = value

    @classmethod
    def get(cls, pk):
        """
        Get by primary key.
        """
        d = db.select_one('select * from %s where %s=?' % (cls.__table__, cls.__primary_key__.name), pk)
        return cls(**d) if d else None

    @classmethod
    def find_first(cls, where, *args):
        """
        where查询，返回首个结果
        """
        d = db.select_one('select * from %s %s' % (cls.__table__, where), *args)
        return cls(**d) if d else None

    @classmethod
    def find_by(cls, where, *args):
        L = db.select('select * from %s %s' % (cls.__table__, where), *args)
        return [cls(**d) for d in L]

    @classmethod
    def find_all(cls):
        L = db.select('select * from %s' % cls.__table__)
        return [cls(**d) for d in L]

    @classmethod
    def count_all(cls):
        return db.select('select count(%s) from %s' % (cls.__primary_key__.name, cls.__table__))

    @classmethod
    def count_by(cls, where, *args):
        return db.select('select count(%s) from %s %s' % (cls.__primary_key__.name, cls.__table__, where), *args)

    @classmethod
    def insert(cls):
        fields = []
        params = []
        args = []
        for k, v in cls.__mappings__.iteritems():
            fields.append(v.name)
            params.append('?')
            args.append(getattr(cls, k, None))
        sql = 'insert into %s (%s) values (%s)' % (cls.__table__, ','.join(fields), ','.join(params))
        db.insert(sql, args)

class User(Model):
    __table__ = 'test'

    id = IntegerField(primary_key=True)
    name = StringField()
    email = StringField()
    passwd = StringField()
    last_modified = FloatField()

if __name__=='__main__':
    db.create_engine('www-data', 'www-data', 'test')

    # # 创建一个实例：
    u = User(id=12345, name='Michael', email='test@orm.org', passwd='my-pwd')
    # # 保存到数据库：
    print u.find_all()
    # u.insert()
    # u.prints()