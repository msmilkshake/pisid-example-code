## Install the MongoDB Replica Set:
- Extract the contents of [replicaimdb.zip](replicaimdb.zip) into C:/

### Commands to start the replica set:

```sh
mongod --config C:\replicaimdb\server1\server1.conf
mongod --config C:\replicaimdb\server2\server2.conf
mongod --config C:\replicaimdb\server3\server3.conf
```

## MQTT needs a Broker running:
So we install and run Mosquitto:
https://mosquitto.org/download/

## Quick Windows Terminal Tutorial
- To split a terminal to the right press:
`Shift + Alt + '+'` (Not the NUMPAD +)
- To split a terminal down press:
    `Shift + Alt + '-'` (Not the NUMPAD -)
- To adjust terminal window size:
  `Shift + Alt + Arrow Key`