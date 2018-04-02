package uk.co.bitcat.accumulo;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Condition;
import org.apache.accumulo.core.data.ConditionalMutation;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import uk.co.bitcat.dto.Transaction;
import uk.co.bitcat.dto.TransactionInput;
import uk.co.bitcat.dto.TransactionOutput;

import java.util.Map;

public class AccumuloWriter {

    private static final String TABLE_NAME = "blockchain";
    private Connector conn;

    public AccumuloWriter() throws Exception {
        String instanceName = "miniInstance";
        String zooKeepers = "localhost:50274";

        Instance instance = new ZooKeeperInstance(instanceName, zooKeepers);

        conn = instance.getConnector("root", new PasswordToken("password"));
        if (conn.tableOperations().exists(TABLE_NAME)) {
            conn.tableOperations().delete(TABLE_NAME);
        }
        conn.tableOperations().create(TABLE_NAME);
    }

    public void write(final Transaction tx) throws Exception {

        if (!isDoubleSpend(tx)) {

            try (BatchWriter writer = conn.createBatchWriter(TABLE_NAME, new BatchWriterConfig())) {

                //TODO::Follow the guidance at
                //TODO::https://accumulo.apache.org/1.8/accumulo_user_manual.html#_running_client_code
                //TODO::section 4.3 'Writing Data' to create the mutation that represents the transaction.
                //TODO::Use a column visibility of ColumnVisibility(), i.e. do not pass a value in the constructor, so
                //TODO::no security label is applied.
                //TODO::Note that you will invoke mutation.put() several times to add all the details of the transaction.
                //TODO::Convert all integers to strings as it will make the viewing of the transaction in the
                //TODO::Mini Accumulo Cluster shell easier for testing.

                //Mutation mutation = new Mutation(...
                //...
                //writer.addMutation(mutation);

            }
        }
    }

    private boolean isDoubleSpend(final Transaction tx) throws Exception {
        boolean doubleSpend = false;

        try (ConditionalWriter cwriter = conn.createConditionalWriter(TABLE_NAME, new ConditionalWriterConfig())) {

            //TODO::Check the transaction inputs to ensure they haven't already been spent.
            //TODO::Use a ConditionalMutation to do this, checking the UTXO is available before writing back the
            //TODO::UTXO column indicating it has now been taken (if it was available).
            //TODO::Set the doubleSpend flag accordingly.
            //TODO::Lines 188 to 192 of https://github.com/apache/accumulo-examples/blob/master/src/main/java/org/apache/accumulo/examples/reservations/ARS.java
            //TODO::show how this could be achieved.

        }

        return doubleSpend;
    }

}
