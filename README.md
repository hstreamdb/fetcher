# README

A command line tool to read all data from a given stream.

```go
./fetcher -h

fetcher is a command line tool for consuming data from HStreamDB Cluster.

Usage:
  fetcher [flags]

Flags:
  -t, --ack-timeout int32        ack timeout in seconds. (default 60)
  -c, --consumer-name string     consumer name.
  -h, --help                     help for fetcher
  -p, --host string              address of the HStream-server, separated by commas. e.g. 127.0.0.1:6570,127.0.0.2:6570,127.0.0.3:6570 (default "127.0.0.1:6570")
  -i, --interval int32           report statistical information in seconds. intervals less than and equal to 0 means report after all done. (default 3)
  -n, --stream-name string       name of stream to subscribe
  -s, --subscription-id string   subscription id.
  -v, --verbose                  print every fetched records if true.
  -w, --wait int32               if no more data fetched in wait seconds, then terminate. -1 means never stop. (default 60)
```

- These `Flags` are required:
  - `host`: The address information of the `HStreamDB-server`. Can be filled with multiple server addresses at the same time, separated by commas. 
  - `stream-name`：Specify the stream to subscribe to
  - `subscription-id`：Set a unique id for the subscription

- `verbose`：When this option is turned on, each message received will be printed in real time
  - Note:  When this option is turned on, the `interval` option will no longer take effect, but will print the total number of entries received after the final execution is completed