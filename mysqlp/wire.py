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
