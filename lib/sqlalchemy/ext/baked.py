from ..orm.query import QueryContext, Query
from ..orm import strategies, attributes, properties, \
    strategy_options, util as orm_util, interfaces
from .. import log as sqla_log
from ..sql import util as sql_util
from ..orm import exc as orm_exc
from .. import util

import copy
import logging

log = logging.getLogger(__name__)


class BakedQuery(object):
    _global_bakery = {}

    __slots__ = 'steps', '_bakery', '_cache_key', '_spoiled'

    def __init__(self, initial_fn, args=(), bakery=None):
        if args:
            self._cache_key = tuple(args)
        else:
            self._cache_key = ()
        self._update_cache_key(initial_fn)
        self.steps = [initial_fn]
        self._spoiled = False
        if bakery is not None:
            self._bakery = bakery
        else:
            self._bakery = self._global_bakery

    def _clone(self):
        b1 = BakedQuery.__new__(BakedQuery)
        b1._cache_key = self._cache_key
        b1.steps = list(self.steps)
        b1._bakery = self._bakery
        b1._spoiled = self._spoiled
        return b1

    def _update_cache_key(self, fn, args=()):
        self._cache_key += (
            fn.func_code.co_filename,
            fn.func_code.co_firstlineno) + args

    def __iadd__(self, other):
        if isinstance(other, tuple):
            self.add_criteria(*other)
        else:
            self.add_criteria(other)
        return self

    def __add__(self, other):
        if isinstance(other, tuple):
            return self.with_criteria(*other)
        else:
            return self.with_criteria(other)

    @classmethod
    def baked(cls, fn):
        def decorate(*args):
            return BakedQuery(fn, args)
        return decorate

    def add_criteria(self, fn, *args):
        self._update_cache_key(fn, args)
        self.steps.append(fn)
        return self

    def with_criteria(self, fn, *args):
        return self._clone().add_criteria(fn, *args)

    def for_session(self, session):
        return Result(self, session)

    def __call__(self, session):
        return self.for_session(session)

    def spoil(self):
        """Cancel any query caching that will occur on this BakedQuery object.

        The BakedQuery can continue to be used normally, however when it
        actually iterates results, no caching will be used.

        This is to support the case where a particular step in constructing
        a baked query disqualifies the query from being cacheable, such
        as a variant that relies upon some uncacheable value.

        """
        self._spoiled = True
        return self

    def _bake(self, session):
        query = self._as_query(session)

        context = query._compile_context()
        self._bake_subquery_loaders(session, context)
        context.session = None
        context.query = query = context.query.with_session(None)
        query._execution_options = query._execution_options.union(
            {"compiled_cache": self._bakery}
        )
        # we'll be holding onto the query for some of its state,
        # so delete some compilation-use-only attributes that can take up
        # space
        for attr in (
                '_correlate', '_from_obj', '_mapper_adapter_map',
                '_joinpath', '_joinpoint'):
            query.__dict__.pop(attr, None)
        self._bakery[self._cache_key] = context

    def _as_query(self, session):
        query = self.steps[0](session)

        for step in self.steps[1:]:
            query = step(query)
        return query

    def _bake_subquery_loaders(self, session, context):
        context.attributes['baked_queries'] = baked_queries = []
        for k, v in context.attributes.items():
            if isinstance(v, Query):
                if 'subquery' in k:
                    bk = BakedQuery(lambda *args: v)
                    bk._cache_key = self._cache_key + k
                    bk._bake(session)
                    baked_queries.append((k, bk._cache_key, v))
                del context.attributes[k]

    def _unbake_subquery_loaders(self, session, context, params):
        for k, cache_key, query in context.attributes["baked_queries"]:
            bk = BakedQuery(lambda sess: query.with_session(sess))
            bk._cache_key = cache_key
            context.attributes[k] = bk.for_session(session).params(params)


class Result(object):
    __slots__ = 'bq', 'session', '_params'

    def __init__(self, bq, session):
        self.bq = bq
        self.session = session
        self._params = {}

    def params(self, **kw):
        self._params.update(**kw)
        return self

    def __iter__(self):
        bq = self.bq
        if bq._spoiled:
            return iter(bq._as_query(self.session))

        if bq._cache_key not in bq._bakery:
            bq._bake(self.session)

        baked_context = bq._bakery[bq._cache_key]

        context = copy.copy(baked_context)
        context.session = self.session
        context.attributes = context.attributes.copy()

        bq._unbake_subquery_loaders(self.session, context, self._params)

        context.statement.use_labels = True
        if context.autoflush and not context.populate_existing:
            self.session._autoflush()
        return context.query.params(self._params).\
            with_session(self.session)._execute_and_instances(context)

    def first(self):
        baked = self.bq.with_params(self._params)
        self.bq.add_criteria(lambda q: q.slice(0, 1))
        ret = list(baked.for_session(self.session))
        if len(ret) > 0:
            return ret[0]
        else:
            return None

    def all(self):
        return list(self)

    def get(self, ident):
        query = self.bq.steps[0](self.session)
        return query._get_impl(ident, self._load_on_ident)

    def _load_on_ident(self, query, key):
        """Load the given identity key from the database."""

        ident = key[1]

        mapper = query._mapper_zero()

        _get_clause, _get_params = mapper._get_clause

        def setup(query):
            _lcl_get_clause = _get_clause
            q = query._clone()
            q._get_condition()
            q._order_by = None

            # None present in ident - turn those comparisons
            # into "IS NULL"
            if None in ident:
                nones = set([
                    _get_params[col].key for col, value in
                    zip(mapper.primary_key, ident) if value is None
                ])
                _lcl_get_clause = sql_util.adapt_criterion_to_null(
                    _lcl_get_clause, nones)

            _lcl_get_clause = q._adapt_clause(_lcl_get_clause, True, False)
            q._criterion = _lcl_get_clause
            return q

        # cache the query against a key that includes
        # which positions in the primary key are NULL
        # (remember, we can map to an OUTER JOIN)
        bq = self.bq

        bq = bq.with_criteria(setup, tuple(elem is None for elem in ident))

        params = dict([
            (_get_params[primary_key].key, id_val)
            for id_val, primary_key in zip(ident, mapper.primary_key)
        ])

        bq.update_params(**params)

        result = list(bq.for_session(self.session))
        l = len(result)
        if l > 1:
            raise orm_exc.MultipleResultsFound()
        elif l:
            return result[0]
        else:
            return None


def bake_lazy_loaders():
    properties.RelationshipProperty.strategy_for(
        lazy="select")(BakedLazyLoader)
    properties.RelationshipProperty.strategy_for(
        lazy=True)(BakedLazyLoader)


def unbake_lazy_loaders():
    properties.RelationshipProperty.strategy_for(
        lazy="select")(strategies.LazyLoader)
    properties.RelationshipProperty.strategy_for(
        lazy=True)(strategies.LazyLoader)


@sqla_log.class_logger
@properties.RelationshipProperty.strategy_for(lazy="baked_select")
class BakedLazyLoader(strategies.LazyLoader):

    def _emit_lazyload(self, session, state, ident_key, passive):

        q = BakedQuery(
            lambda session: session.query(self.mapper),
            bakery=self.mapper._compiled_cache)
        q.add_criteria(
            lambda q: q._adapt_all_clauses()._with_invoke_all_eagers(False),
            self.parent_property)

        if not self.parent_property.bake_queries:
            q.spoil()

        if self.parent_property.secondary is not None:
            q.add_criteria(
                lambda q:
                q.select_from(self.mapper, self.parent_property.secondary))

        pending = not state.key

        # don't autoflush on pending
        if pending or passive & attributes.NO_AUTOFLUSH:
            q.add_criteria(lambda q: q.autoflush(False))

        if state.load_path:
            q.spoil()
            q.add_criteria(
                lambda q:
                q._with_current_path(state.load_path[self.parent_property]))

        if state.load_options:
            q.spoil()
            q.add_criteria(lambda q: q._conditional_options(*state.load_options))

        if self.use_get:
            return q._load_on_ident(q.query, ident_key)

        if self.parent_property.order_by:
            q.add_criteria(
                lambda q:
                q.order_by(*util.to_list(self.parent_property.order_by)))

        for rev in self.parent_property._reverse_property:
            # reverse props that are MANYTOONE are loading *this*
            # object from get(), so don't need to eager out to those.
            if rev.direction is interfaces.MANYTOONE and \
                rev._use_get and \
                    not isinstance(rev.strategy, strategies.LazyLoader):
                q.add_criteria(
                    lambda q:
                    q.options(
                        strategy_options.Load(rev.parent).lazyload(rev.key)))

        lazy_clause, params = self._generate_lazy_clause(state, passive)

        if pending:
            if orm_util._none_set.intersection(params.values()):
                return None

        q.add_criteria(lambda q: q.filter(lazy_clause))

        result = q(session).params(**params).all()
        if self.uselist:
            return result
        else:
            l = len(result)
            if l:
                if l > 1:
                    util.warn(
                        "Multiple rows returned with "
                        "uselist=False for lazily-loaded attribute '%s' "
                        % self.parent_property)

                return result[0]
            else:
                return None
