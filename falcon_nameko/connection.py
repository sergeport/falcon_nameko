from datetime import datetime
from uuid import uuid4


class Connection:
    def __init__(self, connection, recycle_interval):
        self.connection = connection
        self.recycle_interval = recycle_interval
        self.created = datetime.now()
        self.id = uuid4()

    @property
    def must_be_recycled(self) -> bool:
        return self.recycle_interval and (datetime.now() - self.created) > self.recycle_interval

    def __getattr__(self, attr):
        return getattr(self.connection, attr)
