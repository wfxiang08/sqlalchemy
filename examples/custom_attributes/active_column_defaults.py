from sqlalchemy import event


def configure_listener(mapper, class_):
    """Establish attribute setters for every default-holding column on the
    given mapper."""

    for col_attr in mapper.column_attrs:
        column = col_attr.columns[0]
        if column.default is not None:
            default_listener(col_attr, column.default)


def default_listener(col_attr, default):
    """Establish a default-setting listener.

    Given a class_, attrname, and a :class:`.ColumnDefault` instance.

    """
    @event.listens_for(col_attr, "init_scalar", retval=True, propagate=True)
    def init_scalar(target, value, dict_):

        if default.is_callable:
            # the callable of ColumnDefault always accepts a context
            # argument; we can pass it as None here.
            value = default.arg(None)
        elif default.is_clause_element or default.is_sequence:
            # the feature can't easily support this.   This
            # can be made to return None, rather than raising,
            # or can procure a connection from an Engine
            # or Session and actually run the SQL, if desired.
            raise NotImplementedError(
                "Can't invoke pre-default for a SQL-level column default")
        else:
            value = default.arg

        dict_[col_attr.key] = value
        return value


if __name__ == '__main__':

    from sqlalchemy import Column, Integer, DateTime, create_engine
    from sqlalchemy.orm import Session
    from sqlalchemy.ext.declarative import declarative_base
    import datetime

    Base = declarative_base()

    event.listen(Base, 'mapper_configured', configure_listener, propagate=True)

    class Widget(Base):
        __tablename__ = 'widget'

        id = Column(Integer, primary_key=True)

        radius = Column(Integer, default=30)
        timestamp = Column(DateTime, default=datetime.datetime.now)

    e = create_engine("sqlite://", echo=True)
    Base.metadata.create_all(e)

    w1 = Widget()

    # not persisted at all, default values are present
    assert w1.radius == 30
    current_time = w1.timestamp
    assert (
        current_time > datetime.datetime.now() - datetime.timedelta(seconds=5)
    )

    # persist
    sess = Session(e)
    sess.add(w1)
    sess.commit()

    # data is persisted
    assert (
        sess.query(Widget.radius, Widget.timestamp).first() ==
        (30, current_time)
    )
