import pymysql
import datetime
from threading import Lock
from hashlib import md5


class Connection(object):
    def __init__(self, *args, **kargs):
        self.info = {
                     'host': 'localhost',
                     'user': 'root',
                     'passwd': '',
                     'db': '',
                     'port': 3306
                     }
        if kargs.get('host'):
            self.info['host'] = kargs['host']
        if kargs.get('user'):
            self.info['user'] = kargs['user']
        if kargs.get('passwd'):
            self.info['passwd'] = kargs['passwd']
        if kargs.get('db'):
            self.info['db'] = kargs['db']
        if kargs.get('port'):
            self.info['port'] = int(kargs['port'])
        if kargs.get('connect_timeout'):
            self.info['connect_timeout'] = kargs['connect_timeout']
        if kargs.get('use_unicode'):
            self.info['use_unicode'] = kargs['use_unicode']
        if kargs.get('charset'):
            self.info['charset'] = kargs['charset']
        if kargs.get('local_infile'):
            self.info['local_infile'] = kargs['local_infile']
        if kargs.get('username'):
            self.info['user'] = kargs['username']
        if kargs.get('password'):
            self.info['passwd'] = kargs['password']
        if kargs.get('schema'):
            self.info['db'] = kargs['schema']

        if kargs.get('commitOnEnd'):
            self.commitOnEnd = kargs['commitOnEnd']
        else:
            self.commitOnEnd = False

        hashStr = ''.join([str(x) for x in self.info.values()])
        self.key = md5(hashStr).hexdigest()

    def __getattr__(self, name):
        try:
            return self.info[name]
        except Exception:
            return None

    def getKey(self):
        return self.key


connection_timeout = datetime.timedelta(seconds=20)


class ConnectionManager(object):

    def __init__(self, connectionInfo):
        self.connectionInfo = connectionInfo
        self.connection = None

        self._lock = Lock()
        self._locked = False

        self.activeConnections = 0
        self.query = None
        self.lastConnectionCheck = None

    def lock(self):
        self._locked = True
        return self._lock.acquire()

    def release(self):
        if self._locked is True:
            self._locked = False
            self._lock.release()

    def is_locked(self):
        return self._locked

    def cursor(self):
        if self.connection is None:
            self.connect()

        return self.connection.cursor()

    def _updateCheckTime(self):
        self.lastConnectionCheck = datetime.datetime.now()

    def connect(self):
        if self.connection is None:
            self.connection = pymysql.connect(*[], **self.connectionInfo.info)

        if self.connectionInfo.commitOnEnd is True:
            self.connection.autocommit()

        self._updateCheckTime()

    def autoCommit(self, autocommit):
        self.connectionInfo.commitOnEnd = autocommit
        if autocommit is True and self.connection is not None:
            self.connection.autocommit()

    def reconnect(self):
        self.Close()
        self.Connect()

    def test_connection(self, forceCheck=False):
        if self.connection is None:
            return False
        elif (forceCheck is True or
              (datetime.datetime.now() - self.lastConnectionCheck) >=
              connection_timeout):
            try:
                if self.connection.open():
                    self._updateCheckTime()
                    return True
            except Exception:
                self.connection.close()
                self.connection = None
                return False
        else:
            return True

    def commit(self):
        try:
            if self.connection is not None:
                self.connection.commit()
                self._updateCheckTime()
                self.release()
        except Exception:
            pass

    def rollback(self):
        try:
            if self.connection is not None:
                self.connection.rollback()
                self._updateCheckTime()
                self.release()
        except Exception:
            pass

    def close(self):
        if self.connection is not None:
            try:
                self.connection.commit()
                self.connection.close()
                self.connection = None
            except Exception:
                pass
