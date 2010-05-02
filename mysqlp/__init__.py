"""An attempt to write a pure-python MySQL client, which should easily then be
adapted to coro.

:Authors: kylev
"""

import datetime
import hashlib
import logging
import math
import socket
import time

from mysqlp import cursors
from mysqlp import hack
from mysqlp import util
from mysqlp import wire


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
    | CAPS['CLIENT_PROTOCOL_41'] | CAPS['CLIENT_SECURE_CONNECTION']
# Eventually: client_flag |= CAPS['CLIENT_MULTI_STATEMENTS'] | CAPS['CLIENT_MULTI_RESULTS']

# TODO better done a different way?
Error = util.Error
Warning = util.Warning
InterfaceError = util.InterfaceError
DatabaseError = util.DatabaseError
InternalError = util.InternalError
OperationalError = util.OperationalError
ProgrammingError = util.ProgrammingError
IntegrityError = util.IntegrityError
DataError = util.DataError
NotSupportedError = util.NotSupportedError


# TODO Make this if-coro-ish?
def _make_socket(*args):
    s = socket.socket(*args)
    return s


def _extract_int(data, length=1):
    """Extract an n-byte integer, return it and the remaining string."""
    return wire.decode_int(data, length)


def _encode_int(number, length=1):
    return wire.encode_int(number, length)


def _encode_len(length):
    """Encode the length of a string to follow."""
    if length <= 250:
        return chr(length)
    raise NotImplementedError("Length coding incomplete.")


def _len_bin(data):
    """Return a length coded binary string."""
    if data is None:
        # Special NULL column value
        return chr(251)
    return _encode_len(len(data)) + data


class _RandomState:
    """Pseudo-random number generator for old-style passwords (from password.c)."""

    def __init__(self, seed, seed2):
        self.max_value = 0x3FFFFFFF
        self.seed = seed % self.max_value
        self.seed2 = seed2 % self.max_value
        return None

    def rnd(self):
        self.seed = (self.seed * 3 + self.seed2) % self.max_value
        self.seed2 = (self.seed + self.seed2 + 33) % self.max_value
        return float(self.seed) / float(self.max_value)


def _hash_password_323(password):
    nr = 1345345333L
    nr2 = 0x12345671L
    add = 7

    for ch in password:
        if (ch == ' ') or (ch == '\t'):
            continue
        tmp = ord(ch)
        nr ^= (((nr & 63) + add) * tmp) + (nr << 8)
        nr2 += ((nr2 << 8) ^ nr)
        add += tmp

    return (nr & ((1L << 31) - 1L), nr2 & ((1L << 31) - 1L))


def _scramble_323(message, password):
    hash_pass = _hash_password_323(password)
    hash_mess = _hash_password_323(message)

    r = _RandomState(hash_pass[0] ^ hash_mess[0], hash_pass[1] ^ hash_mess[1])
    to = []

    for ch in message:
        to.append(int(math.floor((r.rnd() * 31) + 64)))

    extra = int(math.floor(r.rnd() * 31))
    for i in range(len(to)):
        to[i] ^= extra

    return ''.join([chr(x) for x in to])


def _scramble(message, password):
    # Double SHA1 the password
    stage_one = hashlib.sha1(password).digest()
    stage_two = hashlib.sha1(stage_one).digest()
    # Combine the two
    to = hashlib.sha1(message + stage_two).digest()

    # XOR together
    xored = [chr(ord(a) ^ ord(b)) for a, b in zip(stage_one, to)]
    return ''.join(xored)


class Connection(object):
    # Expose all the exceptions as connection attributes (PEP 249 optional)
    Error = util.Error
    Warning = util.Warning
    InterfaceError = util.InterfaceError
    DatabaseError = util.DatabaseError
    InternalError = util.InternalError
    OperationalError = util.OperationalError
    ProgrammingError = util.ProgrammingError
    IntegrityError = util.IntegrityError
    DataError = util.DataError
    NotSupportedError = util.NotSupportedError

    def __init__(self, user='', password='', host='localhost', database=None, port=3306):
        self._log = logging.getLogger(self.__class__.__name__)
        self._user = user
        self._password = password
        self._host = host
        self._db = database
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

        self._log.debug("ipkt%d:%s", seq, hack.hexify(data))

        return seq, data

    def _read_reply_header(self):
        self._seq, data = self._read_packet()

        code, data = _extract_int(data)
        if code == 255:
            errnum, rest = _extract_int(data, 2)
            sqlstate = rest[1:5]
            errmsg = rest[6:]
            raise InterfaceError('%d (%s) - %s' % (errnum, sqlstate, errmsg))
        return code, data

    def _send_packet(self, data, seq=0):
        if not self._s:
            raise Error('Attempt to run a command on a closed connection.')

        if seq > 0:
            self._seq = seq
        else:
            self._seq += 1
        packet = '%s%s%s' % (_encode_int(len(data), 3), _encode_int(self._seq),
                             data)

        self._log.debug("Sending packet %s", hack.hexify(packet))

        if self._s.sendall(packet) is not None:
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

        # Empty 13 bytes, then 12 more bytes of the salt
        rest = rest[13:]
        self._salt += rest[:12]

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

        client_flag = DEFAULT_CAPS

        if self._db:
            client_flag |= CAPS['CLIENT_CONNECT_WITH_DB']

        # Clear abilities the server doesn't have.
        client_flag = ((client_flag &
                        ~(CAPS['CLIENT_COMPRESS'] | CAPS['CLIENT_SSL'] | CAPS['CLIENT_PROTOCOL_41'])) |
                       (client_flag & self._capabilities))

        login_pkt = _encode_int(client_flag, 2)
        if client_flag & CAPS['CLIENT_PROTOCOL_41']:
            # Long flag
            login_pkt += _encode_int(client_flag >> 16, 2)
        login_pkt += "%s%s%s\x00" % ("\x00\x00\x00\x01\x08",
                                     "\x00" * 23,
                                     self._user,
                                     )

        if self._password:
            enc_password = _scramble(self._salt, self._password)
            login_pkt += _len_bin(enc_password)
        else:
            login_pkt += '\x00'

        if self._db:
            login_pkt = '%s%s\x00' % (login_pkt, self._db)

        # Login packet always has seqence 1 for some reason
        self._send_packet(login_pkt, 1)
        code, data = self._read_reply_header()

        if code == 0xfe:
            self._log.debug("Fallback encoding requested.")
            self._send_packet(_scramble_323(self._salt[:8], self._password)[:8] + '\x00')
            code, data = self._read_reply_header()

    def close(self):
        self._send_packet('\x01')
        # No response
        self._s.close()
        self._s = None

    def commit(self):
        self._cmd_query('COMMIT')
        # TODO Error handling
        self._read_reply_header()

    def rollback(self):
        self._cmd_query('COMMIT')
        # TODO Error handling
        self._read_reply_header()

    def cursor(self):
        return cursors.Cursor(self)

    def _cmd_query(self, query):
        self._send_packet('\x03' + query)


# PEP-249 Required alias
connect = Connection

# TODO Define all the other constants and singletons from PEP 249

# Constructors
def Date(year, month, day):
    return datetime.date(year, month, day)

def Time(hour, minute, second):
    return datetime.time(hour, minute, second)

def Timestamp(year, month, day, hour, minute, second):
    datetime.datetime(year, month, day, hour, minute, second)

def DateFromTicks(ticks):
    return Date(*time.localtime(ticks)[:3])

def TimeFromTicks(ticks):
    return Time(*time.localtime(ticks)[3:6])

def TimestampFromTicks(ticks):
    return Timestamp(*time.localtime(ticks)[:6])

def Binary(value):
    return str(value)


# Type objects
class _DBAPIType(frozenset):
    def __eq__(self, other):
        return other in self

STRING = _DBAPIType((wire.MYSQL_TYPE_ENUM, wire.MYSQL_TYPE_VAR_STRING,
                     wire.MYSQL_TYPE_STRING))
BINARY = _DBAPIType()
NUMBER = _DBAPIType()
DATETIME = _DBAPIType()
ROWID = _DBAPIType()
