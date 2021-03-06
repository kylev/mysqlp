"""Cursor implementation."""

import logging

from mysqlp import hack
from mysqlp import util
from mysqlp import wire


EOF_MARK = '\xfe'

def _str_or_null(x):
    if x is None:
        return None
    return str(x)


# TODO Better decoding strategy from MySQL to python types.
_decoders = {0: int, 1: int, 2: int, 3: int, 8: long, 0xfe: _str_or_null, 0xfd: _str_or_null}
_log = logging.getLogger(__name__)

class Cursor(object):
    """A cursor, though which most database interactions are done."""

    def __init__(self, conn):
        self._conn = conn # Should I do something weakref-ish?
        self._data = None
        self.rowcount = -1
        self.arraysize = 1
        self._result_fields = None
        self._result_rows = None
        self._insert_id = None

    @property
    def description(self):
        """Description of the cursor results.  It will be either None (if the last
        statement didn't return results) or a sequence of 7-tuples containing:

        (name, type_code, display_size, internal_size, precision, scale, null_ok)
        """
        return self._result_fields

    @property
    def connection(self):
        """Read-only access to the connection object."""
        return self._conn

    @property
    def lastrowid(self):
        """Row ID of the last inserted row, or None."""
        return self._insert_id

    def callproc(self, procname, params=None):
        raise NotImplementedError("TODO")

    def execute(self, stmt, params=None):
        _log.debug("execute: '%s'", stmt)
        self._result_fields = None
        self._result_rows = None
        self.rowcount = -1

        # TODO Actual escaping !
        if params:
            esc_ed = ["'%s'" % (x.replace('\'', '\\\''),) for x in params]
            self._conn._cmd_query(stmt % tuple(esc_ed))
        else:
            self._conn._cmd_query(stmt)
        seq, data = self._conn._read_packet()
        field_count, data = wire.decode_int(data)

        if field_count == 0xff:
            errno, rest = wire.decode_int(data, 2)
            sql_state = rest[:5]
            message = rest[5:]
            raise util.OperationalError(message, errno)
        if field_count == 0:
            return

        # Read the field descriptions
        self._result_fields = list()
        while True:
            seq, data = self._conn._read_packet()
            if data[0] == EOF_MARK:
                break
            # TODO Decode the field data?
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

            # print hack.hexify(rest[2:])
            # Only keeping a subset I actually need to know about
            # TODO I'll probably need to know more later.
            self._result_fields.append(
                (name, col_type, length, None, decimals, None, flags & 0x1)
                )

        if len(self._result_fields) != field_count:
            raise util.InternalError("Crap, wrong number of fields")

        # Read the result rows
        self._result_rows = list()
        while True:
            seq, data = self._conn._read_packet()
            if data[0] == EOF_MARK:
                break
            columns = list()
            while data:
                col, data = wire.decode_lstr(data)
                columns.append(col)
            self._result_rows.append(tuple(columns))
        self.rowcount = len(self._result_rows)

    def close(self):
        self._result_rows = None

    def executemany(self, stmt, param_seq=None):
        # TODO more efficient
        for params in param_seq:
            self.execute(stmt, params)

    def _decode_row(self, row):
        decoded = list()
        for i in range(len(row)):
            decoded.append(_decoders[self._result_fields[i][1]](row[i]))
        return tuple(decoded)

    def fetchone(self):
        if self.rowcount == -1:
            raise util.ProgrammingError("Called with no available results.")
        if self.rowcount == 0:
            return None
        if not self._result_rows:
            return None

        return self._decode_row(self._result_rows.pop(0))

    def fetchmany(self, size=None):
        if self.rowcount == -1:
            raise util.ProgrammingError("Called with no available results.")
        if self.rowcount == 0 or not self._result_rows:
            return list()

        if size == -1:
            size = len(self._result_rows)
        elif size is None:
            size = self.arraysize
        result = [self._decode_row(x) for x in self._result_rows[:size]]
        del self._result_rows[:size]
        return result or list()

    def fetchall(self):
        return self.fetchmany(-1)

    def nextset(self):
        raise NotImplementedError("TODO")

    def setinputsizes(self, sizes):
        """Does not apply to MySQL."""
        pass

    def setoutputsize(self, size, column=None):
        """Does not apply to MySQL."""
        pass

    def __del__(self):
        self.close()

