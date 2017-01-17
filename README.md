# zkpipe

Consume Zookeeper binary log to sync filtered transactions to Kafka topic.

# Features

- [Sync](#sync) transactions from `Zookeeper` binary log files to `Kafka` topic. 
- [Watch](#watch) log directory and continue to sync changed transactions. 
- [Filter](#filter) transaction with `zxid range` or `path prefix`.
- [Encode](#encoding-format) transaction as `JSON`, `ProtoBuf` or `Raw` format.
- Resume [from latest](#from-latest) transaction in the `Kafka` topic.
- Report [metrics](#metrics) to `Prometheus`, `Graphite` or `Ganglia`.

# Usage

[watch](#watch) and [sync](#sync) command consume changed or [filtered](#filter) transactions from the Zookeeper binary log files, 
send [encoded](#encoding-format) message to Kafka topic that defined in the `--kafka-uri` option.

## Watch 
`watch` log directory and continue to sync changed transactions to kafka that defined in the `--kafka-uri` option.

> zkpipe watch \<log dir\> --kafka-uri \<uri\>

```bash
$ zkpipe watch /usr/local/var/run/zookeeper/data/version-2/ --kafka-uri=kafka://localhost/zkpipe 
```

### From Latest
By default, `zkpipe` will auto resume the sync progress from the latest transaction in the Kafka topic, 
`--from-latest` option skip the sync progress to the latest zxid in the Zookeeper binary logs. 

> zkpipe watch \<log dir\> [options] --from-latest

```bash
$ zkpipe watch /usr/local/var/run/zookeeper/data/version-2/ --kafka-uri=kafka://localhost/zkpipe --from-latest
```

## Sync 
`sync` transactions from the binary log files to Kafka topic.

> zkpipe sync \<log files\>

```bash
$ zkpipe sync /usr/local/var/run/zookeeper/data/version-2/log.c7
```

When the syntax is "glob" then the `log files` representation of the path is matched using a limited pattern language that resembles regular expressions but with a simpler syntax. For example:

- *.java	Matches a path that represents a file name ending in .java
- *.*	Matches file names containing a dot
- *.{java,class}	Matches file names ending with .java or .class
- foo.?	Matches file names starting with foo. and a single character extension
- /home/*/*	Matches /home/gus/data on UNIX platforms
- /home/**	Matches /home/gus and /home/gus/data on UNIX platforms
- C:\\*	Matches C:\foo and C:\bar on the Windows platform (note that the backslash is escaped; as a string literal in the Java Language the pattern would be "C:\\\\*")

Please check [FileSystem.getPathMatcher](https://docs.oracle.com/javase/7/docs/api/java/nio/file/FileSystem.html#getPathMatcher(java.lang.String)) document for more detail. 

## Filter 

`watch` or `sync` command can filter transactions with filters.

### zxid Range

`--range <zxid:zxid>` option filter transaction in the `zxid` range
- `<low>:` matches transactions `zxid` lager than `low` value
- `:<high>` matches transactions `zxid` less than `high` value
- `<low>:<high>` matches transactions `zxid` between `low` and `high` value
- `:` matches all transactions

> zkpipe [options] --range 238:250

### Path Prefix

`--prefix <path>` option filter transaction act on the node that have same path prefix.

> zkpipe [options] --prefix /broker

### Match Pattern

`--match <pattern>` option filter transaction act on the node that path matches pattern in the regular expression. 

> zkpipe [options] --match ^/broker/.*

### Session ID

`--session-id <id>` option filter transaction with the session ID

> zkpipe [options] --session-id 97295928260820996

### Ignore Session Events

`--ignore-session` option ignore all the Zookeeper session events, including CreateSession, CloseSession etc.

> zkpipe [options] --ignore-session

## Encoding Format

`--encode <format>` support the following encoding

- `pb` encode transaction in ProtoBuf format
- `json` encode transaction in JSON format
- `raw` transaction in raw Zookeeper format 

> zkpipe [options] --encode pb

# Metrics

`zkpipe` will serve metrics for Prometheus scrape or reports metrics to Graphite or Ganglia server.

## Prometheus

`zkpipe` serve metrics for [Prometheus](https://prometheus.io/) scrape in pull mode, that defined in the `--metrics-uri <uri>` option defined. 

> zkpipe watch \<log dir\> [options] --metrics-uri http://<host>\[:port\]/<topic>

When deploy `zkpipe` behind firewall or without public IP address, use `--push-gateway <addr>` option to push metrics to the Prometheus [push gateway](https://prometheus.io/docs/practices/pushing/).

> zkpipe watch \<log dir\> [options] --push-gateway <host>\[:port\]

## Graphite

`zkpipe` report metrics to the [Graphite](https://graphiteapp.org/) server that defined in the `--report-uri <uri>` option.

> zkpipe watch \<log dir\> --kafka-uri \<uri\> --report-uri graphite://<host>\[:port\]

The [pickle protocol](http://graphite.readthedocs.io/en/latest/feeding-carbon.html#the-pickle-protocol) is a much more efficient take on the plaintext protocol, and supports sending batches of metrics.

> zkpipe watch \<log dir\> [options] --report-uri pickle://<host>\[:port\]

## Ganglia

`zkpipe` report metrics to the [Ganglia](http://ganglia.info/) server that defined in the `--report-uri <uri>` option.

> zkpipe watch \<log dir\> [options] --report-uri ganglia://<host>\[:port\]

# Records

`zkpipe` support all the Zookeeper transaction type, please check ProtoBuf schema for more detail.

## Create Session
```json
{
  "cxid": 0,
  "record": {
    "create-session": {
      "timeout": 30000
    }
  },
  "session": 97250703064170497,
  "time": 1484217490213,
  "type": "CreateSession",
  "zxid": 10
}
```
## Create Node
```json
{
  "cxid": 5,
  "path": "/test/abc",
  "record": {
    "create": {
      "acl": [
        {
          "id": "anyone",
          "perms": 31,
          "scheme": "world"
        }
      ],
      "data": "",
      "ephemeral": false,
      "parentCVersion": 1,
      "path": "/test/abc"
    }
  },
  "session": 97223867455045632,
  "time": 1483674202631,
  "type": "Create",
  "zxid": 3
}
```
## Set Data
```json
{
  "cxid": 2,
  "path": "/test/abc",
  "record": {
    "set-data": {
      "data": "aGVsbG8=",
      "path": "/test/abc",
      "version": 3
    }
  },
  "session": 97250703064170497,
  "time": 1484218456704,
  "type": "SetData",
  "zxid": 12
}
```