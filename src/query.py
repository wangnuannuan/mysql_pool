import time
from .pool import Pool
import logging


class PySQLQuery(object):
    def __init__(self, conn):
        self.connInfo = conn
        self.record = {}
        self.rowcount = 0
        self.affectedRows = None
        self.conn = None
        self.lastError = None
        self.lastInsertID = None

    def __del__(self):
        if self.conn is not None:
            self._returnconnection()

    def __enter__(self):

        self.query('START TRANSACTION')
        logging.info('Starting Transaction')

    def __exit__(self, exc_type):
        if exc_type is None:
            self.query('COMMIT')
            logging.info('Commiting Transaction')
        else:
            self.query('ROLLBACK')
            logging.info('Rolling Back Transaction')

    def query(self, query, args=None):
        self.affectedRows = None
        self.lastError = None
        cursor = None

        try:
            try:
                self._getconnection()

                logging.debug('Running query "%s" with args "%s"', query, args)
                self.conn.query = query

                cursor = self.conn.cursor()
                with self.conn.cursor() as cursor:
                    cursor.execute(query, args)
                    self.record = cursor.fetchall()
                self.conn.updateCheckTime()
            except Exception as e:
                print(e)
        finally:
            if cursor is not None:
                cursor.close()
            self.__returnconnection()

    def _getconnection(self):
        while self.conn is None:
            self.conn = Pool().connection(self.connInfo)
            if self.conn is not None:
                break
            else:
                time.sleep(1)

    def _returnconnection(self):
        if self.conn is not None:
            if self.connInfo.commitOnEnd is True or self.commitOnEnd is True:
                self.conn.Commit()

            Pool().returnConnection(self.conn)
            self.conn = None
