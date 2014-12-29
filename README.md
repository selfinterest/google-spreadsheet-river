google-spreadsheet-river
========================

A module to synchronize a Google spreadsheet with a redis store. That is: suppose you are storing data for a web app in a Google spreadsheet.

You don't want to read the spreadsheet all the bloody time. You want to read it once an hour and store the data in something quick and temporary like redis. That's what this module is for.

API
----
