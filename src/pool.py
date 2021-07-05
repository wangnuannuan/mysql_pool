from threading import Condition
from .connection import ConnectionManager


class Pool(object):

    __Pool = dict()

    def __init__(self, maxActiveConnections=10):
        self.__dict__ = self.__Pool
        self.maxActiveConnections = maxActiveConnections

        if 'lock' not in self.__dict__:
            self.lock = Condition()

        if 'connections' not in self.__dict__:
            self.connections = dict()

    def terminate(self):
        self.lock.acquire()
        try:
            for bucket in self.connections.values():
                try:
                    for conn in bucket:
                        conn.lock()
                        try:
                            conn.close()
                        except Exception as e:
                            print(e)
                            pass
                        conn.release()
                except Exception as e:
                    print(e)
                    pass
            self.connections = dict()
        finally:
            self.lock.release()

    def cleanup(self):
        self.lock.acquire()
        try:
            for bucket in self.connections.values():
                try:
                    for conn in bucket:
                        conn.lock()
                        try:
                            open = conn.testconnection()
                            if open is True:
                                conn.commit()
                            else:
                                index = bucket.index(conn)
                                del bucket[index]
                            conn.release()
                        except Exception as e:
                            print(e)
                            conn.release()
                except Exception:
                    pass
        finally:
            self.lock.release()

    def commit(self):
        self.lock.acquire()
        try:
            for bucket in self.connections.values():
                try:
                    for conn in bucket:
                        conn.lock()
                        try:
                            conn.commit()
                            conn.release()
                        except Exception as e:
                            print(e)
                            conn.release()
                except Exception:
                    pass
        finally:
            self.lock.release()

    def getconnection(self, conn):
        key = conn.getKey()

        connection = None

        if key in self.connections:
            connection = self._getConnectionFromPoolSet(key)

            if connection is None:
                self.lock.acquire()
                if len(self.connections[key]) < self.maxActiveConnections:
                    connection = self._create_connection(conn)
                    self.connections[key].append(connection)
                    self.lock.release()
                else:
                    while connection is None:
                        connection = self._getConnectionFromPoolSet(key)
                    self.lock.release()
        else:
            self.lock.acquire()
            if key not in self.connections:
                self.connections[key] = []

            if len(self.connections[key]) < self.maxActiveConnections:
                connection = self._create_connection(conn)
                self.connections[key].append(connection)
            else:
                while connection is None:
                    connection = self._getConnectionFromPoolSet(key)
            self.lock.release()

        return connection

    def _getConnectionFromPoolSet(self, key):
        connection = None

        for conn in self.connections[key]:
            if not conn.is_locked():
                conn.lock()
                try:
                    if conn.testconnection() is False:
                        conn.reconnect()

                    connection = conn
                    conn.release()
                except Exception:
                    conn.release()
                    raise

        return connection

    def _create_connection(self, info):
        connection = ConnectionManager(info)
        connection.connect()

        return connection
