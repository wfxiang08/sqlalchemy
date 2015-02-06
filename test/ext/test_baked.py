from sqlalchemy.orm import Session
from sqlalchemy.testing import eq_, is_, is_not_
from test.orm import _fixtures
from sqlalchemy.ext.baked import BakedQuery
from sqlalchemy import bindparam, func
import itertools


class BakedTest(_fixtures.FixtureTest):
    run_setup_mappers = 'once'
    run_inserts = 'once'
    run_deletes = None

    @classmethod
    def setup_mappers(cls):
        cls._setup_stock_mapping()


class StateChangeTest(BakedTest):
    def setup(self):
        self._cache = {}

    def test_initial_key(self):
        User = self.classes.User
        session = Session()
        l1 = lambda: session.query(User)
        q1 = BakedQuery(l1, bakery=self._cache)
        eq_(
            q1._cache_key,
            (l1.func_code.co_filename, l1.func_code.co_firstlineno)
        )
        eq_(q1.steps, [l1])

    def test_inplace_add(self):
        User = self.classes.User
        session = Session()
        l1 = lambda: session.query(User)
        l2 = lambda q: q.filter(User.name == bindparam('name'))
        q1 = BakedQuery(l1, bakery=self._cache)
        eq_(
            q1._cache_key,
            (l1.func_code.co_filename, l1.func_code.co_firstlineno)
        )
        eq_(q1.steps, [l1])

        q2 = q1.add_criteria(l2)
        is_(q2, q1)

        eq_(
            q1._cache_key,
            (l1.func_code.co_filename, l1.func_code.co_firstlineno) +
            (l2.func_code.co_filename, l2.func_code.co_firstlineno)
        )
        eq_(q1.steps, [l1, l2])

    def test_inplace_add_operator(self):
        User = self.classes.User
        session = Session()
        l1 = lambda: session.query(User)
        l2 = lambda q: q.filter(User.name == bindparam('name'))
        q1 = BakedQuery(l1, bakery=self._cache)
        eq_(
            q1._cache_key,
            (l1.func_code.co_filename, l1.func_code.co_firstlineno)
        )

        q1 += l2

        eq_(
            q1._cache_key,
            (l1.func_code.co_filename, l1.func_code.co_firstlineno) +
            (l2.func_code.co_filename, l2.func_code.co_firstlineno)
        )

    def test_chained_add(self):
        User = self.classes.User
        session = Session()
        l1 = lambda: session.query(User)
        l2 = lambda q: q.filter(User.name == bindparam('name'))
        q1 = BakedQuery(l1, bakery=self._cache)

        q2 = q1.with_criteria(l2)
        is_not_(q2, q1)

        eq_(
            q1._cache_key,
            (l1.func_code.co_filename, l1.func_code.co_firstlineno)
        )
        eq_(
            q2._cache_key,
            q1._cache_key +
            (l2.func_code.co_filename, l2.func_code.co_firstlineno)
        )

    def test_chained_add_operator(self):
        User = self.classes.User
        session = Session()
        l1 = lambda: session.query(User)
        l2 = lambda q: q.filter(User.name == bindparam('name'))
        q1 = BakedQuery(l1, bakery=self._cache)

        q2 = q1 + l2
        is_not_(q2, q1)

        eq_(
            q1._cache_key,
            (l1.func_code.co_filename, l1.func_code.co_firstlineno)
        )
        eq_(
            q2._cache_key,
            q1._cache_key +
            (l2.func_code.co_filename, l2.func_code.co_firstlineno)
        )


class ResultTest(BakedTest):
    def test_no_steps(self):
        User = self.classes.User

        bq = BakedQuery(
            lambda s: s.query(User.id, User.name).order_by(User.id))

        for i in range(3):
            session = Session()
            eq_(
                bq(session).all(),
                [(7, 'jack'), (8, 'ed'), (9, 'fred'), (10, 'chuck')]
            )

    def test_w_new_entities(self):
        """Test that the query can have its entities modified in
        an arbitrary callable, and that this new entity list is preserved
        when the query is invoked.

        """
        User = self.classes.User

        bq = BakedQuery(
            lambda s: s.query(User.id, User.name))

        bq += lambda q: q.from_self().with_entities(
            func.count(User.id))

        for i in range(3):
            session = Session()
            eq_(
                bq(session).all(),
                [(4, )]
            )

    def test_conditional_step(self):
        """Test a large series of conditionals and assert that
        results remain correct between all of them within a series
        of loops.

        """
        User = self.classes.User

        base_bq = BakedQuery(
            lambda s: s.query(User.id, User.name))

        base_bq += lambda q: q.order_by(User.id)

        for i in range(4):
            for cond1, cond2, cond3, cond4 in itertools.product(
                    *[(False, True) for j in range(4)]):
                bq = base_bq._clone()
                if cond1:
                    bq += lambda q: q.filter(User.name != 'jack')
                    if cond2:
                        bq += lambda q: q.join(User.addresses)
                    else:
                        bq += lambda q: q.outerjoin(User.addresses)
                elif cond3:
                    bq += lambda q: q.filter(User.name.like('%ed%'))
                else:
                    bq += lambda q: q.filter(User.name == 'jack')

                if cond4:
                    bq += lambda q: q.from_self().with_entities(
                        func.count(User.id))
                sess = Session()
                result = bq(sess).all()
                if cond4:
                    if cond1:
                        if cond2:
                            eq_(result, [(4,)])
                        else:
                            eq_(result, [(5,)])
                    elif cond3:
                        eq_(result, [(2,)])
                    else:
                        eq_(result, [(1,)])
                else:
                    if cond1:
                        if cond2:
                            eq_(
                                result,
                                [(8, 'ed'), (8, 'ed'), (8, 'ed'),
                                 (9, 'fred')]
                            )
                        else:
                            eq_(
                                result,
                                [(8, 'ed'), (8, 'ed'), (8, 'ed'),
                                 (9, 'fred'), (10, 'chuck')]
                            )
                    elif cond3:
                        eq_(result, [(8, 'ed'), (9, 'fred')])
                    else:
                        eq_(result, [(7, 'jack')])
