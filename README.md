# nf26-metar

[METAR](https://en.wikipedia.org/wiki/METAR)

## Connect to server
Se placer dans le répertoire local où se trouve le directory que l'on souhaite monter vers le serveur puis :
```bash
$ sshfs login@nf26-3.leger.tf: directory
$ ssh login@nf26-3.leger.tf
```

## Changer son mot de passe
```
$ passwd login
```

## cqlsh
```
$ cqlsh
$ cqlsh> CREATE KEYSPACE login_demo_td76 WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2};
$ cqlsh> use login_demo_td76 ;
```

## Exécuter un fichier python
```
$ ipython
$ In [1]: %run test.py
```