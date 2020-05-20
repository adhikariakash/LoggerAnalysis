# LoggerAnalysis
One of the most common uses of Spark is analyzing and processing log files. In this assignment, we will put Spark to good use for an OSS project that retrieves and downloads data from GitHub, called GHTorrent. GHTorrent works by following the Github event timeline and then retrieving all items linked from each event recursively and exhaustively. To make monitoring and debugging easier, the GHTorrent maintainers use extensive runtime logging for the downloader scripts.

Here is an extract of what the GHTorrent log looks like:

DEBUG, 2017-03-23T10:02:27+00:00, ghtorrent-40 -- ghtorrent.rb: Repo EFForg/https-everywhere exists
DEBUG, 2017-03-24T12:06:23+00:00, ghtorrent-49 -- ghtorrent.rb: Repo Shikanime/print exists
INFO, 2017-03-23T13:00:55+00:00, ghtorrent-42 -- api_client.rb: Successful request. URL: https://api.github.com/repos/CanonicalLtd/maas-docs/issues/365/events?per_page=100, Remaining: 4943, Total: 88 ms
WARN, 2017-03-23T20:04:28+00:00, ghtorrent-13 -- api_client.rb: Failed request. URL: https://api.github.com/repos/greatfakeman/Tabchi/commits?sha=Tabchi&per_page=100, Status code: 404, Status: Not Found, Access: ac6168f8776, IP: 0.0.0.0, Remaining: 3031
DEBUG, 2017-03-23T09:06:09+00:00, ghtorrent-2 -- ghtorrent.rb: Transaction committed (11 ms)
Each log line comprises of a standard part (up to .rb:) and an operation-specific part. The standard part fields are like so:

Logging level, one of DEBUG, INFO, WARN, ERROR (separated by ,)
A timestamp (separated by ,)
The downloader id, denoting the downloader instance (separated by --)
The retrieval stage, denoted by the Ruby class name, one of:
event_processing
ght_data_retrieval
api_client
retriever
ghtorrent

Loading and Parsing
For the remaining of the assignement, you need to use this file https://drive.google.com/file/d/0B9Rx0uhucsroYWJxdEpPd2JYcjg/view?usp=sharing (~300MB compressed).

Basic Analysis
How many rows does the table loggers contain?
Count the number of WARNING messages
How many repositories were processed in total? Use the api_client lines only.
Which client did most HTTP requests?
Which client did most FAILED HTTP requests? Use group_by to provide an answer.
What is the most active hour of the day?
What is the most active repository (use messages from the ghtorrent.rb layer only)?
Which access keys are failing most often? (extract the Access: ... part from failing requests)?

Joining
We now need to monitor the behavior of interesting repositories. Use this link https://drive.google.com/open?id=0B9Rx0uhucsroRHNVTFpzMV9OUGs to download a list of repos into which we are interested to. The following list was generated more than 7 months after the log file was created. The format of the file is CSV.
Compress the csv file and upload the compressed file into another hive table called interesting.

How many records are there in the table interesting?
How many records in the logger table refer to entries in the interesting table?
Which of the interesting repositories has the most failed API calls?
