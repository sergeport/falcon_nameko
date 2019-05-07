from typing import Callable, Union, List, NoReturn
from datetime import timedelta
from threading import Lock
from six.moves.queue import Queue, Empty
from .errors import ClientUnavailableError, WrongConnectionProducerError
from .connection import Connection


class ConnectionPool:

    def __init__(self, connection_producer: Callable, min_connections: int = 2, max_connections: int = 8,
                 recycle_frequency: int = 0):
        """
        Create a new connections pool
        :param func connection_producer: The callable that returns Connection instance
        :param int min_connections: Minimum number of connections to create
        :param int max_connections: Maximum number of connections to create (in case if there is no free connection)
        constructor
        """
        self.queue: Queue = Queue()
        self.recycle_frequency: int = timedelta(seconds=recycle_frequency) if recycle_frequency > 0 else 0
        self.total: int = 0
        self.max_connections: int = max_connections
        self._lock: Lock = Lock()
        self._connection_producer: Callable = connection_producer
        self._make(min_connections)

    def _make(self, connections_count: int = 1) -> Union[Connection, List[Connection]]:
        created: List[Connection] = []
        for x in range(connections_count):
            c = Connection(self._connection_producer(), self.recycle_frequency)
            if not isinstance(c, Connection):
                raise WrongConnectionProducerError('Connection producer must return instance of Connection class')
            self.queue.put(c)
            created.append(c)
            print(f'New connection created and queued in pool: {c.id}')
        self.total += len(created)
        return created.pop() if connections_count == 1 else created

    def _delete(self, connection) -> NoReturn:
        print(f'Connection {connection.id} deleted from pool')
        del connection
        self.total -= 1

    def _recycle_if_needed(self, connection) -> Connection:
        if connection.must_be_recycled:
            self._lock.acquire()
            print(f'Recycling pool connection {connection.id}')
            self._delete(connection)
            connection = self._make()
            self._lock.release()
        return connection

    def next(self) -> Connection:
        return self._recycle_if_needed(self._double_attempt_pooled_connection())

    def _double_attempt_pooled_connection(self, timeout_1: Union[float, int] = 0.05,
                                          timeout_2: Union[float, int] = 1) -> Connection:
        try:
            return self.queue.get(True, timeout_1)
        except Empty:
            try:
                self._lock.acquire()
                if self.total >= self.max_connections:
                    try:
                        return self.queue.get(True, timeout_2)
                    except Empty:
                        raise ClientUnavailableError("Too many connections")
                return self._make()
            finally:
                self._lock.release()

    def release(self, connection: Connection) -> NoReturn:
        self.queue.put(connection, True)
