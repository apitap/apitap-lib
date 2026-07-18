# e2e fixtures for the `github://` source

These CSVs are read back FROM GitHub by the end-to-end tests
(`github://apitap/apitap-lib/tests/data`): RFC-4180 edge cases — quoted
commas, embedded LF/CRLF newlines, doubled quotes, unicode, empty fields
(NULL), a short row (NULL-padded), a blank line — plus a second table for
multi-table runs. Do not "fix" the odd rows; they are the test.
