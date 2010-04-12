CREATE DATABASE mysqlp_test;
GRANT ALL ON mysqlp_test.* TO 'testuser1'@'localhost' IDENTIFIED BY 'pass1';
-- "pass2" as old style hash.
GRANT ALL ON mysqlp_test.* TO 'testuser2'@'localhost' IDENTIFIED BY PASSWORD '136b4c537575b6f1';
