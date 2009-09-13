"""Cursor implementation."""

from mysqlp import wire
from mysqlp import hack


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
            catalog, rest = wire.decode_lstr(data)
            db, rest = wire.decode_lstr(rest)
            table, rest = wire.decode_lstr(rest)
            org_table, rest = wire.decode_lstr(rest)
            name, rest = wire.decode_lstr(rest)
            org_name, rest = wire.decode_lstr(rest)
            # Skip filler byte
            charset, rest = wire.decode_int(rest[1:], 2)
            length, rest = wire.decode_int(rest, 4)
            col_type, rest = wire.decode_int(rest)
            flags, rest =  wire.decode_int(rest, 2)
            decimals, rest =  wire.decode_int(rest)
            print hack.hexify(rest[2:])

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

