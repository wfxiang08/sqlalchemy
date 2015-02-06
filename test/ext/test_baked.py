from sqlalchemy.orm import Session, subqueryload, mapper, relationship
from sqlalchemy.testing import eq_, is_, is_not_, assert_raises
from sqlalchemy import testing
from test.orm import _fixtures
from sqlalchemy.ext.baked import BakedQuery, baked_lazyload
from sqlalchemy import bindparam, func
from sqlalchemy.orm import exc as orm_exc
import itertools


class BakedTest(_fixtures.FixtureTest):
    run_setup_mappers = 'once'
    run_inserts = 'once'
    run_deletes = None


class StateChangeTest(BakedTest):
    @classmethod
    def setup_mappers(cls):
        User = cls.classes.User

        mapper(User, cls.tables.users)

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


class LikeQueryTest(BakedTest):
    @classmethod
    def setup_mappers(cls):
        User = cls.classes.User

        mapper(User, cls.tables.users)

    def test_one_no_result(self):
        User = self.classes.User

        bq = BakedQuery(lambda s: s.query(User))
        bq += lambda q: q.filter(User.name == 'asdf')

        assert_raises(
            orm_exc.NoResultFound,
            bq(Session()).one
        )

    def test_one_multiple_result(self):
        User = self.classes.User

        bq = BakedQuery(lambda s: s.query(User))
        bq += lambda q: q.filter(User.name.like('%ed%'))

        assert_raises(
            orm_exc.MultipleResultsFound,
            bq(Session()).one
        )

    def test_get(self):
        User = self.classes.User

        bq = BakedQuery(lambda s: s.query(User))

        sess = Session()

        def go():
            u1 = bq(sess).get(7)
            eq_(u1.name, 'jack')
        self.assert_sql_count(testing.db, go, 1)

        u1 = sess.query(User).get(7)  # noqa

        def go():
            u2 = bq(sess).get(7)
            eq_(u2.name, 'jack')
        self.assert_sql_count(testing.db, go, 0)

        def go():
            u2 = bq(sess).get(8)
            eq_(u2.name, 'ed')
        self.assert_sql_count(testing.db, go, 1)

    def test_get_pk_w_null(self):
        """test the re-implementation of logic to do get with IS NULL."""

        class AddressUser(object):
            pass
        mapper(
            AddressUser,
            self.tables.users.outerjoin(self.tables.addresses),
            properties={
                "id": self.tables.users.c.id,
                "address_id": self.tables.addresses.c.id
            }
        )

        bq = BakedQuery(lambda s: s.query(AddressUser))

        sess = Session()

        def go():
            u1 = bq(sess).get((10, None))
            eq_(u1.name, 'chuck')
        self.assert_sql_count(testing.db, go, 1)

        u1 = sess.query(AddressUser).get((10, None))  # noqa

        def go():
            u2 = bq(sess).get((10, None))
            eq_(u2.name, 'chuck')
        self.assert_sql_count(testing.db, go, 0)


class ResultTest(BakedTest):
    @classmethod
    def setup_mappers(cls):
        User = cls.classes.User
        Address = cls.classes.Address

        mapper(User, cls.tables.users, properties={
            "addresses": relationship(
                Address, order_by=cls.tables.addresses.c.id)
        })
        mapper(Address, cls.tables.addresses)

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

    def test_spoiled_w_params(self):
        User = self.classes.User

        bq = BakedQuery(
            lambda s: s.query(User.id, User.name).order_by(User.id))

        bq += lambda q: q.filter(User.id == bindparam('id'))

        sess = Session()
        eq_(
            bq.spoil()(sess).params(id=7).all(),
            [(7, 'jack')]
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

                sess.close()

    def test_subquery_eagerloading(self):
        User = self.classes.User
        Address = self.classes.Address

        base_bq = BakedQuery(
            lambda s: s.query(User))

        base_bq += lambda q: q.options(subqueryload(User.addresses))
        base_bq += lambda q: q.order_by(User.id)

        assert_result = [
            User(id=7, addresses=[
                Address(id=1, email_address='jack@bean.com')]),
            User(id=8, addresses=[
                Address(id=2, email_address='ed@wood.com'),
                Address(id=3, email_address='ed@bettyboop.com'),
                Address(id=4, email_address='ed@lala.com'),
            ]),
            User(id=9, addresses=[
                Address(id=5)
            ]),
            User(id=10, addresses=[])
        ]

        for i in range(4):
            for cond1, cond2 in itertools.product(
                    *[(False, True) for j in range(2)]):
                bq = base_bq._clone()

                sess = Session()

                if cond1:
                    bq += lambda q: q.filter(User.name == 'jack')
                else:
                    bq += lambda q: q.filter(User.name.like('%ed%'))

                if cond2:
                    ct = func.count(Address.id).label('count')
                    subq = sess.query(
                        ct,
                        Address.user_id).group_by(Address.user_id).\
                        having(ct > 2).subquery()

                    bq += lambda q: q.join(subq)

                if cond2:
                    if cond1:
                        def go():
                            result = bq(sess).all()
                            eq_([], result)
                        self.assert_sql_count(testing.db, go, 1)
                    else:
                        def go():
                            result = bq(sess).all()
                            eq_(assert_result[1:2], result)
                        self.assert_sql_count(testing.db, go, 2)
                else:
                    if cond1:
                        def go():
                            result = bq(sess).all()
                            eq_(assert_result[0:1], result)
                        self.assert_sql_count(testing.db, go, 2)
                    else:
                        def go():
                            result = bq(sess).all()
                            eq_(assert_result[1:3], result)
                        self.assert_sql_count(testing.db, go, 2)

                sess.close()


class LazyLoaderTest(BakedTest):
    run_setup_mappers = 'each'

    def test_baked_lazy_loading_o2m(self):
        User = self.classes.User
        Address = self.classes.Address

        mapper(User, self.tables.users, properties={
            'addresses': relationship(
                Address, order_by=self.tables.addresses.c.id)
        })
        mapper(Address, self.tables.addresses)

        base_bq = BakedQuery(
            lambda s: s.query(User))

        base_bq += lambda q: q.options(baked_lazyload(User.addresses))
        base_bq += lambda q: q.order_by(User.id)

        assert_result = [
            User(id=7, addresses=[
                Address(id=1, email_address='jack@bean.com')]),
            User(id=8, addresses=[
                Address(id=2, email_address='ed@wood.com'),
                Address(id=3, email_address='ed@bettyboop.com'),
                Address(id=4, email_address='ed@lala.com'),
            ]),
            User(id=9, addresses=[
                Address(id=5)
            ]),
            User(id=10, addresses=[])
        ]

        for i in range(4):
            for cond1, cond2 in itertools.product(
                    *[(False, True) for j in range(2)]):
                bq = base_bq._clone()

                sess = Session()

                if cond1:
                    bq += lambda q: q.filter(User.name == 'jack')
                else:
                    bq += lambda q: q.filter(User.name.like('%ed%'))

                if cond2:
                    ct = func.count(Address.id).label('count')
                    subq = sess.query(
                        ct,
                        Address.user_id).group_by(Address.user_id).\
                        having(ct > 2).subquery()

                    bq += lambda q: q.join(subq)

                if cond2:
                    if cond1:
                        def go():
                            result = bq(sess).all()
                            eq_([], result)
                        self.assert_sql_count(testing.db, go, 1)
                    else:
                        def go():
                            result = bq(sess).all()
                            eq_(assert_result[1:2], result)
                        self.assert_sql_count(testing.db, go, 2)
                else:
                    if cond1:
                        def go():
                            result = bq(sess).all()
                            eq_(assert_result[0:1], result)
                        self.assert_sql_count(testing.db, go, 2)
                    else:
                        def go():
                            result = bq(sess).all()
                            eq_(assert_result[1:3], result)
                        self.assert_sql_count(testing.db, go, 3)

                sess.close()

    def test_baked_lazy_loading_m2o(self):
        User = self.classes.User
        Address = self.classes.Address

        mapper(User, self.tables.users)
        mapper(Address, self.tables.addresses, properties={
            'user': relationship(User)
        })

        base_bq = BakedQuery(
            lambda s: s.query(Address))

        base_bq += lambda q: q.options(baked_lazyload(Address.user))
        base_bq += lambda q: q.order_by(Address.id)

        assert_result = self.static.address_user_result

        for i in range(4):
            for cond1 in (False, True):
                bq = base_bq._clone()

                sess = Session()

                if cond1:
                    bq += lambda q: q.filter(
                        Address.email_address == 'jack@bean.com')
                else:
                    bq += lambda q: q.filter(
                        Address.email_address.like('ed@%'))

                if cond1:
                    def go():
                        result = bq(sess).all()
                        eq_(assert_result[0:1], result)
                    self.assert_sql_count(testing.db, go, 2)
                else:
                    def go():
                        result = bq(sess).all()
                        eq_(assert_result[1:4], result)
                    self.assert_sql_count(testing.db, go, 2)

                sess.close()
