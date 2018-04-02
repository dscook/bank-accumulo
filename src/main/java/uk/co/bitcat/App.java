package uk.co.bitcat;

import uk.co.bitcat.accumulo.AccumuloWriter;
import uk.co.bitcat.dto.Transaction;
import uk.co.bitcat.kafka.TransactionConsumer;

import java.util.List;

public class App
{
    public static void main(String[] args) throws Exception {
        AccumuloWriter accumuloWriter = new AccumuloWriter();

        try (TransactionConsumer txConsumer = new TransactionConsumer()) {
            while (true) {
                // Receive transactions
                List<Transaction> transactions = txConsumer.receive();

                // Write each transaction to Accumulo
                for (Transaction tx : transactions) {
                    accumuloWriter.write(tx);
                }
            }
        }
    }
}
