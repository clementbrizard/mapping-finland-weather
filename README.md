# nf26-metar

[METAR](https://en.wikipedia.org/wiki/METAR)

## Connect to server
```
$ ssh login@nf26-3.leger.tf
$ cqlsh
$ cqlsh> CREATE KEYSPACE login_demo_td76 WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2};
$ cqlsh> use login_demo_td76 ;

```

## Mount distant server
```
sftp://login@nf26-3.leger.tf/home/<local login>
```