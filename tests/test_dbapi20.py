"""Subclass the dbapi20 unit test suite to exercise the mysqlp driver."""

import dbapi20
import mysqlp


class MySQLpTest(dbapi20.DatabaseAPI20Test):
    driver = mysqlp
    connect_kw_args = dict(user='kylev', db='test')

    def test_setoutputsize(self):
        """Skipped."""
        pass

    def test_nextset(self):
        """Skipped."""
        pass

if __name__ == '__main__':
    import logging, unittest
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()
