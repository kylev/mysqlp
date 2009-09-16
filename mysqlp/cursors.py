"""Cursor implementation."""

import logging

from mysqlp import hack
from mysqlp import util
from mysqlp import wire


EOF_MARK = '\xfe'

# TODO Better decoding strategy from MySQL to python types.
_decoders = {0: int, 1: int, 2: int, 3: int, 8: long, 0xfe: str, 0xfd: str}
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

    @property
    def description(self):
        """7-tuple description of the cursor."""
        # TODO (name, type_code, display_size, internal_size, precision,
        # scale, null_ok)
        return ('name', 'typecode', None, None, None, None, None, None)

    def callproc(self, procname, params):
        raise NotImplementedError("TODO")

    def execute(self, stmt, params=None):
        _log.debug("execute: '%s'", stmt)
        self._conn._cmd_query(stmt)
        seq, data = self._conn._read_packet()
        field_count = ord(data[0])

        if field_count == 0xff:
            raise util.OperationalError()
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
            self._result_fields.append((col_type, decimals))

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

    def close(self):
        self._result_rows = None

    def executemany(self, stmt, params=None):
        raise NotImplementedError("TODO")

    def _decode_row(self, row):
        decoded = list()
        for i in range(len(row)):
            decoded.append(_decoders[self._result_fields[i][0]](row[i]))
        return tuple(decoded)

    def fetchone(self):
        if not self._result_rows:
            self._result_rows = None
            return None
        return self._decode_row(self._result_rows.pop(0))

    def fetchmany(self, size=None):
        raise NotImplementedError("TODO")

    def fetchall(self):
        result = [self._decode_row(x) for x in self._result_rows]
        self._result_rows = None
        return result

    def nextset(self):
        raise NotImplementedError("TODO")

    def setinputsizes(self, sizes):
        raise NotImplementedError("TODO")

    def setoutputsize(self, size, column=None):
        raise NotImplementedError("TODO")

    def __del__(self):
        self.close()

