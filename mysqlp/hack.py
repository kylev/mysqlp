"""Little stuff useful for hacking on the library.  Do not use."""

def hexify(data):
    return ' '.join(['%02x' % (ord(x),) for x in data])
