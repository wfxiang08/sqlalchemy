# -*- coding: utf-8 -*-
# engine/strategies.py
# Copyright (C) 2005-2015 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""Strategies for creating new instances of Engine types.

These are semi-private implementation classes which provide the
underlying behavior for the "strategy" keyword argument available on
:func:`~sqlalchemy.engine.create_engine`.  Current available options are
``plain``, ``threadlocal``, and ``mock``.

New strategies can be added via new ``EngineStrategy`` classes.
"""

from operator import attrgetter
from sqlalchemy.dialects.mysql.pymysql import MySQLDialect_pymysql

from sqlalchemy.engine import base, threadlocal, url
from sqlalchemy import util, exc, event
from sqlalchemy import pool as poollib

strategies = {}


class EngineStrategy(object):
    """An adaptor that processes input arguments and produces an Engine.

    Provides a ``create`` method that receives input arguments and
    produces an instance of base.Engine or a subclass.

    """

    def __init__(self):
        strategies[self.name] = self

    def create(self, *args, **kwargs):
        """Given arguments, returns a new Engine instance."""

        raise NotImplementedError()

# DefaultEngineStrategy也不能直接使用: self.name没有指定
class DefaultEngineStrategy(EngineStrategy):
    """Base class for built-in strategies."""

    # SQLALCHEMY_DATABASE_URI, echo=DEBUG, pool_size=CONCURRENCY, max_overflow=2
    def create(self, name_or_url, **kwargs):
        # 1. create url.URL object
        u = url.make_url(name_or_url)

        # 2. mysql+mysqldb ---> dialect = MySQLDialect_mysqldb

        # "mysql+pymysql"
        # entrypoint = u._get_entrypoint()
        # 3. dialect_cls 就是entrypoint的class
        # dialect_cls = entrypoint.get_dialect_cls(u)

        # 实例化:
        dialect_cls = MySQLDialect_pymysql

        if kwargs.pop('_coerce_config', False):
            def pop_kwarg(key, default=None):
                value = kwargs.pop(key, default)
                if key in dialect_cls.engine_config_types:
                    value = dialect_cls.engine_config_types[key](value)
                return value
        else:
            pop_kwarg = kwargs.pop

        dialect_args = {}
        # consume dialect arguments from kwargs
        for k in util.get_cls_kwargs(dialect_cls):
            if k in kwargs:
                dialect_args[k] = pop_kwarg(k)

        # 4. module没有指定，就使用默认的
        dbapi = kwargs.pop('module', None)
        if dbapi is None:
            dbapi_args = {}
            for k in util.get_func_kwargs(dialect_cls.dbapi):
                if k in kwargs:
                    dbapi_args[k] = pop_kwarg(k)

            # 获取dbapi, 例如: MySQLdb
            dbapi = dialect_cls.dbapi(**dbapi_args)

        dialect_args['dbapi'] = dbapi

        # create dialect: MySQLDialect_mysqldb
        dialect = dialect_cls(**dialect_args)

        # assemble connection arguments
        (cargs, cparams) = dialect.create_connect_args(u)
        cparams.update(pop_kwarg('connect_args', {}))
        cargs = list(cargs)  # allow mutability

        # look for existing pool or create
        pool = pop_kwarg('pool', None)
        if pool is None:
            def connect(connection_record=None):
                if dialect._has_events:
                    for fn in dialect.dispatch.do_connect:
                        connection = fn(
                            dialect, connection_record, cargs, cparams)
                        if connection is not None:
                            return connection
                return dialect.connect(*cargs, **cparams)

            creator = pop_kwarg('creator', connect)

            # 默认的Pool为: return getattr(cls, 'poolclass', pool.QueuePool)
            poolclass = pop_kwarg('poolclass', None)
            if poolclass is None:
                poolclass = dialect_cls.get_pool_class(u)
            pool_args = {}

            # consume pool arguments from kwargs, translating a few of
            # the arguments
            translate = {'logging_name': 'pool_logging_name',
                         'echo': 'echo_pool',
                         'timeout': 'pool_timeout',
                         'recycle': 'pool_recycle',
                         'events': 'pool_events',
                         'use_threadlocal': 'pool_threadlocal',
                         'reset_on_return': 'pool_reset_on_return'}
            for k in util.get_cls_kwargs(poolclass):
                tk = translate.get(k, k)
                if tk in kwargs:
                    pool_args[k] = pop_kwarg(tk)
            pool = poolclass(creator, **pool_args)
        else:
            if isinstance(pool, poollib._DBProxy):
                pool = pool.get_pool(*cargs, **cparams)
            else:
                pool = pool

        engineclass = self.engine_cls
        engine_args = {}
        for k in util.get_cls_kwargs(engineclass):
            if k in kwargs:
                engine_args[k] = pop_kwarg(k)

        _initialize = kwargs.pop('_initialize', True)

        # all kwargs should be consumed
        if kwargs:
            raise TypeError(
                "Invalid argument(s) %s sent to create_engine(), "
                "using configuration %s/%s/%s.  Please check that the "
                "keyword arguments are appropriate for this combination "
                "of components." % (','.join("'%s'" % k for k in kwargs),
                                    dialect.__class__.__name__,
                                    pool.__class__.__name__,
                                    engineclass.__name__))

        # 给定pool和dbapi后面的engine是啥呢?
        # create engine.
        engine = engineclass(pool, dialect, u, **engine_args)

        # 创建时即创建连接
        if _initialize:
            do_on_connect = dialect.on_connect()
            if do_on_connect:
                def on_connect(dbapi_connection, connection_record):
                    conn = getattr(
                        dbapi_connection, '_sqla_unwrap', dbapi_connection)
                    if conn is None:
                        return
                    do_on_connect(conn)

                event.listen(pool, 'first_connect', on_connect)
                event.listen(pool, 'connect', on_connect)

            def first_connect(dbapi_connection, connection_record):
                c = base.Connection(engine, connection=dbapi_connection,
                                    _has_events=False)
                c._execution_options = util.immutabledict()
                dialect.initialize(c)
            event.listen(pool, 'first_connect', first_connect, once=True)

        dialect_cls.engine_created(engine)
        if entrypoint is not dialect_cls:
            entrypoint.engine_created(engine)

        return engine


class PlainEngineStrategy(DefaultEngineStrategy):
    """Strategy for configuring a regular Engine."""

    name = 'plain'
    engine_cls = base.Engine

PlainEngineStrategy()

