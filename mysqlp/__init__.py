"""An attempt to write a pure-python MySQL client, which should easily then be
adapted to coro.

:Authors: kylev
"""

import exceptions
import hashlib
import logging
import math
import socket


apilevel = '2.0'
threadsafety = 1
paramstyle = 'format'


DEFAULT_RECV_SIZE = 0x8000
MYSQL_HEADER_SIZE = 0x4

# Stolen from mysql_com.h
CAPS = dict(CLIENT_LONG_PASSWORD=1,
            CLIENT_FOUND_ROWS=2,
            CLIENT_LONG_FLAG=4,
            CLIENT_CONNECT_WITH_DB=8,
            CLIENT_NO_SCHEMA=16,
            CLIENT_COMPRESS=32,
            CLIENT_ODBC=64,
            CLIENT_LOCAL_FILES=128,
            CLIENT_IGNORE_SPACE=256,
            CLIENT_PROTOCOL_41=512,
            CLIENT_INTERACTIVE=1024,
            CLIENT_SSL=2048,
            CLIENT_IGNORE_SIGPIPE=4096,
            CLIENT_TRANSACTIONS=8192,
            CLIENT_RESERVED=16384,
            CLIENT_SECURE_CONNECTION=32768,
            CLIENT_MULTI_STATEMENTS=(1 << 16),
            CLIENT_MULTI_RESULTS=(1 << 17),
            CLIENT_SSL_VERIFY_SERVER_CERT=(1 << 30),
            CLIENT_REMEMBER_OPTIONS=(1 << 31),
            )
DEFAULT_CAPS = CAPS['CLIENT_LONG_PASSWORD'] | CAPS['CLIENT_LONG_FLAG'] \
    | CAPS['CLIENT_SECURE_CONNECTION'] | CAPS['CLIENT_TRANSACTIONS'] \
    | CAPS['CLIENT_PROTOCOL_41']


# PEP 249 required exceptions
class Error(exceptions.StandardError):
    pass

class Warning(exceptions.StandardError):
    pass

class InterfaceError(Error):
    pass

class DatabaseError(Error):
    pass

class InternalError(DatabaseError):
    pass

class OperationalError(DatabaseError):
    pass

class ProgrammingError(DatabaseError):
    pass

class IntegrityError(DatabaseError):
    pass

class DataError(DatabaseError):
    pass

class NotSupportedError(DatabaseError):
    pass


# TODO Make this if-coro-ish?
def _make_socket(*args):
    s = socket.socket(*args)
    return s


def _extract_int(data, length=1):
    """Extract an n-byte integer, return it and the remaining string."""

    result = 0
    for i in xrange(length):
        result |= ord(data[i]) << (i * 8)

    return result, data[length:]


def _encode_int(number, length=1):
    result = ''
    for i in xrange(length):
        result += chr((number >> (i * 8)) & 0xff)
    return result


def _scramble(message, password):
    # Double SHA1 the password
    stage_one = hashlib.sha1(password).digest()
    stage_two = hashlib.sha1(stage_one).digest()

    # Combine the two
    crypt_string = hashlib.sha1(message)
    crypt_string.update(stage_two)
    to = crypt_string.digest()

    # XOR together
    result = ''.join([chr(ord(to[x]) ^ ord(stage_two[x]))
                      for x in xrange(len(to))])

    return result


def _hexify(data):
    return ' '.join(['%02x' % (ord(x),) for x in data])


class Connection(object):
    def __init__(self, host='localhost', user='', passwd='', db=None, port=3306):
        self._log = logging.getLogger(self.__class__.__name__)
        self._user = user
        self._password = passwd
        self._db = db
        self._port = port

        self._s = _make_socket(socket.AF_INET, socket.SOCK_STREAM)
        self._s.connect((host, port))

        self._recv_buffer = ''
        self._recv_length = 0

        self._login()

    def _recv(self):
        """Call to get data when we expect it."""
        data = self._s.recv(DEFAULT_RECV_SIZE)
        if not data:
            raise "TODO exception"
        else:
            self._recv_buffer += data
            self._recv_length += len(data)

    def _get_header(self):
        if self._recv_length < MYSQL_HEADER_SIZE:
            return None, None
        else:
            # 3-byte length, one-byte packet number, followed by packet data
            a, b, c, seq = map(ord, self._recv_buffer[:MYSQL_HEADER_SIZE])
            length = a | (b << 8) | (c << 16)
        return length, seq

    def _read_packet(self):
        packet_len, seq = self._get_header()

        while MYSQL_HEADER_SIZE > self._recv_length \
                or packet_len + MYSQL_HEADER_SIZE > self._recv_length:

            self._recv()
            if packet_len is None:
                # TODO Make this less messy
                packet_len, seq = self._get_header()

        data = self._recv_buffer[MYSQL_HEADER_SIZE:MYSQL_HEADER_SIZE + packet_len]
        self._recv_buffer = self._recv_buffer[MYSQL_HEADER_SIZE + packet_len:]
        self._recv_length = self._recv_length - (MYSQL_HEADER_SIZE + packet_len)

        return seq, data

    def _read_reply_header(self):
        seq, data = self._read_packet()

        code, data = _extract_int(data)
        if code == 255:
            errnum, errmsg = _extract_int(data, 2)
            raise InterfaceError('%d - %s' % (errnum, errmsg))
        elif code == 254:
            raise InterfaceError('Unknown header <%s>' % (repr(data),))
        else:
            print repr(data)

    def _send_packet(self, data):
        self._seq += 1

        packet = '%s%s%s' % (_encode_int(len(data), 3), _encode_int(self._seq),
                             data)

        self._log.debug("Sending packet %s", _hexify(packet))

        if self._s.sendall(data) is not None:
            raise InterfaceError("Send failed.")

    def _decode_greeting(self, greeting):
        """Extract all the info about the server from the greeting packet."""
        # 1-byte protocol version followed by null terminated server version string
        self._proto, rest = _extract_int(greeting)
        self._version, rest = rest.split('\x00', 1)

        # 4-byte thread ID
        self._thread_id, rest = _extract_int(rest, 4)

        # Scramble buff, skip 1-byte filler
        self._salt, rest = rest[:8], rest[9:]

        # Two bytes of capabilities
        self._capabilities, rest = _extract_int(rest, 2)

        # Single charset byte
        self._charset, rest = _extract_int(rest)

        # Two bytes of status
        self._status, rest = _extract_int(rest, 2)

        # Empty 13 bytes, then 13 more bytes of the salt
        self._salt += rest[13:]

        if self._log.isEnabledFor(logging.DEBUG):
            self._log.debug("Protocol v%d, version %s, thread %d, "
                            "charset %d, salt %s, status %d",
                            self._proto, self._version, self._thread_id,
                            self._charset, repr(self._salt), self._status)
            for k, v in CAPS.iteritems():
                has_it = 'no'
                if self._capabilities & v:
                    has_it = 'yes'
                self._log.debug('%s: %s', k, has_it)

    def _login(self):
        self._seq, greeting = self._read_packet()
        self._log.debug("Greeting %s", repr(greeting))
        self._decode_greeting(greeting)

        enc_password = _scramble(self._salt, self._password)
        #self._log.debug("Sending scramble %s length %d", repr(enc_password),
        #                len(enc_password))

        capabilities = DEFAULT_CAPS
        if self._db:
            capabilities |= CAPS['CLIENT_CONNECT_WITH_DB']

        login_pkt = "%s%s%s%s\x00%s%s" % (_encode_int(capabilities, 4),
                                          "\x00\x00\x00\x01\x08",
                                          "\x00" * 23,
                                          self._user,
                                          _encode_int(len(enc_password)),
                                          enc_password)

        if self._db:
            login_pkt = "%s%s\x00" % (login_pkt, self._db)

        print len(login_pkt)

        self._send_packet(login_pkt)
        self._read_reply_header()

    def close(self):
        self._send_packet('\x00')
        # No response
        self._s.close()
        self._s = None

    def commit(self):
        pass

    def rollback(self):
        pass

    def cursor(self):
        return Cursor(self)


# PEP-249 Required alias
connect = Connection


class Cursor(object):
    def __init__(self, connection):
        self._connection = connection
        self.description = None # TODO 7-tuple
        self.rowcount = None
        self.arraysize = 1

    def __del__(self):
        self.close()

    def callproc(self, procname, params=None):
        pass

    def close(self):
        self.connection = None

    def execute(self, operation, params=None):
        pass

    def executemany(self, operation, params_seq):
        pass

    def fetchone(self):
        pass

    def fetchmany(self, qty):
        pass

    def fetchall(self):
        pass

    def nextset(self):
        pass

    def setinputsizes(self, sizes):
        pass

    def setoutputsize(self, size, column=None):
        pass


# TODO Define all the other constants and singletons from PEP 249
