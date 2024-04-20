def get_or_create(session, model, **kwargs):
    # https://stackoverflow.com/questions/2546207/does-sqlalchemy-have-an-equivalent-of-djangos-get-or-create
    instance = session.query(model).filter_by(**kwargs).first()
    if instance:
        return instance
    else:
        instance = model(**kwargs)
        session.add(instance)
        session.commit()
        return instance


class NestedSession:
    """
    Change session for NestedSession in order to make
    standalone transactions part of larger transactions:

    def standalone1(session):
        with session.begin():
            session.add(...)

    def main(session):
        with session.begin():
            standalone1(NestedSession(session))
            standalone2(NestedSession(session))

    """

    def __init__(self, session):
        self.session = session

    def __enter__(self, *args, **kwargs):
        return self

    def __exit__(self, *args, **kwargs):
        return False

    #

    def add(self, *args, **kwargs):
        self.session.add(*args, **kwargs)

    def add_all(self, *args, **kwargs):
        self.session.add_all(*args, **kwargs)

    def begin(self, *args, **kwargs):
        return self
