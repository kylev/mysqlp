"""Wire-format encoding and decoding functions for the MySQL protocol."""

# Field types from mysql_com.h
MYSQL_TYPE_DECIMAL = 0
MYSQL_TYPE_TINY = 1
MYSQL_TYPE_SHORT = 2
MYSQL_TYPE_LONG = 3
MYSQL_TYPE_FLOAT = 4
MYSQL_TYPE_DOUBLE = 5
MYSQL_TYPE_NULL = 6
MYSQL_TYPE_TIMESTAMP = 7
MYSQL_TYPE_LONGLONG = 8
MYSQL_TYPE_INT24 = 9
MYSQL_TYPE_DATE = 10
MYSQL_TYPE_TIME = 11
MYSQL_TYPE_DATETIME = 12
MYSQL_TYPE_YEAR = 13
MYSQL_TYPE_NEWDATE = 14
MYSQL_TYPE_VARCHAR = 15
MYSQL_TYPE_BIT = 16
MYSQL_TYPE_NEWDECIMAL = 246
MYSQL_TYPE_ENUM = 247
MYSQL_TYPE_SET = 248
MYSQL_TYPE_TINY_BLOB = 249
MYSQL_TYPE_MEDIUM_BLOB = 250
MYSQL_TYPE_LONG_BLOB = 251
MYSQL_TYPE_BLOB = 252
MYSQL_TYPE_VAR_STRING = 253
MYSQL_TYPE_STRING = 254
MYSQL_TYPE_GEOMETRY = 255


def decode_lstr(x):
    """Decode the wire-format length coded string."""
    first = ord(x[0])
    if first == 0:
        return None, x[1:]
    return x[1:first + 1], x[first + 1:]


def decode_int(data, length=1):
    """Extract an n-byte integer, return it and the remaining string."""
    result = 0
    for i in xrange(length):
        result |= ord(data[i]) << (i * 8)

    return result, data[length:]


def encode_int(number, length=1):
    """Encode an integer into the wire-format encoding of `length` bytes."""
    result = list()
    for i in xrange(length):
        result.append(chr((number >> (i * 8)) & 0xff))
    return ''.join(result)
