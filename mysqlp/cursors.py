"""Cursor implementation."""

class Cursor(object):
    """A cursor, though which most database interactions are done."""

    def __init__(self, conn):
        self._conn = conn # Should I do something weakref-ish?
        self._data = None
        self.rowcount = -1
        self.arraysize = 1

    @property
    def description(self):
        """7-tuple description of the cursor."""
        # TODO (name, type_code, display_size, internal_size, precision,
        # scale, null_ok)
        return ('name', 'typecode', None, None, None, None, None, None)

    def callproc(self, procname, params):
        pass

    def execute(self, stmt, params=None):
        self._conn._cmd_query(stmt)
        self._data = self._conn._read_reply_header()
        while True:
            seq, data = self._conn._read_packet()
            if data[0] == '\xfe':
                break
        print repr(self._data)

    def close(self):
        pass

    def executemany(self, stmt, params=None):
        pass

    def fetchone(self):
        pass

    def fetchmany(self, size=None):
        pass

    def fetchall(self):
        pass

    def nextset(self):
        pass

    def setinputsizes(self, sizes):
        pass

    def setoutputsize(self, size, column=None):
        pass

    def __del__(self):
        self.close()

