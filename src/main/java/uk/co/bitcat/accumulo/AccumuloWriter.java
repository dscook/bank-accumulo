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

                Mutation mutation = new Mutation(new Text(tx.getId()));
                ColumnVisibility cv = new ColumnVisibility();
                Text cf;
                Text cq;
                Value value;

                for (Map.Entry<Integer, TransactionInput> input : tx.getInputs().entrySet()) {
                    cf = new Text("inputs");

                    cq = new Text(input.getKey().toString() + ":txId");
                    value = new Value(input.getValue().getTxId().getBytes());
                    mutation.put(cf, cq, cv, value);

                    cq = new Text(input.getKey().toString() + ":utxoIndex");
                    value = new Value(Integer.toString(input.getValue().getUtxoIndex()).getBytes());
                    mutation.put(cf, cq, cv, value);

                    cq = new Text(input.getKey().toString() + ":amount");
                    value = new Value(Integer.toString(input.getValue().getAmount()).getBytes());
                    mutation.put(cf, cq, cv, value);

                    cq = new Text(input.getKey().toString() + ":address");
                    value = new Value(input.getValue().getAddress().getBytes());
                    mutation.put(cf, cq, cv, value);
                }

                for (Map.Entry<Integer, TransactionOutput> output : tx.getOutputs().entrySet()) {
                    cf = new Text("outputs");

                    cq = new Text(output.getKey().toString() + ":amount");
                    value = new Value(Integer.toString(output.getValue().getAmount()).getBytes());
                    mutation.put(cf, cq, cv, value);

                    cq = new Text(output.getKey().toString() + ":address");
                    value = new Value(output.getValue().getAddress().getBytes());
                    mutation.put(cf, cq, cv, value);

                    cf = new Text("utxo");
                    cq = new Text(output.getKey().toString());
                    value = new Value("available".getBytes());
                    mutation.put(cf, cq, cv, value);
                }

                writer.addMutation(mutation);
            }
        }
    }

    private boolean isDoubleSpend(final Transaction tx) throws Exception {
        boolean doubleSpend = false;

        try (ConditionalWriter cwriter = conn.createConditionalWriter(TABLE_NAME, new ConditionalWriterConfig())) {
            for (TransactionInput input : tx.getInputs().values()) {

                ConditionalMutation cm = new ConditionalMutation(new Text(input.getTxId()));

                Condition ensureUtxoAvailable = new Condition("utxo", Integer.toString(input.getUtxoIndex()));
                ensureUtxoAvailable.setValue("available");
                cm.addCondition(ensureUtxoAvailable);

                cm.put("utxo", Integer.toString(input.getUtxoIndex()), tx.getId());

                ConditionalWriter.Status status = cwriter.write(cm).getStatus();

                // This is not production code, we would need to release all other inputs we had reserved in the
                // event we could not obtain one of them.  Fortunately all the kafka producer only ever creates
                // transactions with at most one input.
                if (status != ConditionalWriter.Status.ACCEPTED) {
                    System.out.println("Detected double spend for transaction: " + tx.getId());
                    doubleSpend = true;
                    break;
                }
            }
        }

        return doubleSpend;
    }

}
