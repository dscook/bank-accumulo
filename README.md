# Bank to the Future - Accumulo

## Steps

1. Complete the uk.co.bitcat.AccumuloWriter class.  Two methods need implementing, write() and isDoubleSpend().  Initially just focus on the write() method and only attempt isDoubleSpend() if you have time.
1. Start a Mini Accumulo Cluster by following the steps in the section below.
1. Run your Accumulo Client Code as detailed in the section below, every time you run your code it will clear the state held in Accumulo so you will need to additionally restart your Kafka producer code to start sending and storing from tx1 again.  You should not need to restart the Mini Accumulo Cluster.
1. To view what is being stored in Accumulo, in the Mini AccumuloCluster console window type `scan -t blockchain`.  If you are storing the transactions correctly they should look similar to the following:

```
tx11 inputs:0:address []    account4
tx11 inputs:0:txid []    tx4
tx11 inputs:0:utxoIndex []    0
tx11 outputs:0:address []    account8
tx11 outputs:0:amount []    40
tx11 outputs:1:address []    account5
tx11 outputs:1:amount []    60
tx11 utxo:0 []    tx23
tx11 utxo:1 []    available
```

## To Run a Mini Accumulo Cluster

* Right click the uk.co.bitcat.accumulo.AccumuloCluster class in an IDE such as IntelliJ or Eclipse and click run.
* Alternatively run `mvn -Pcluster exec:java` in a terminal in the same directory as this README after a `mvn clean install`.

## To Run the Accumulo Client Code

* Right click the App class in an IDE such as IntelliJ or Eclipse and click run.
* Alternatively run `mvn exec:java` in a terminal in the same directory as this README after a `mvn clean install`.