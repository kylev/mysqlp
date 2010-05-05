"""Subclass the dbapi20 unit test suite to exercise the mysqlp driver."""

import dbapi20
import mysqlp


class MySQLpTest(dbapi20.DatabaseAPI20Test):
    driver = mysqlp
    connect_kw_args = dict(user='kylev', database='test')

    def test_setoutputsize(self):
        """Skipped."""
        pass

    def test_nextset(self):
        """Skipped."""
        pass


if __name__ == '__main__':
    import logging, sys, unittest
    if '-v' in sys.argv:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.WARNING)
    unittest.main()
