package uk.co.bitcat.dto;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Transaction {

    private String id;
    private Map<Integer, TransactionInput> inputs;
    private Map<Integer, TransactionOutput> outputs;

    public Transaction() {};

    public Transaction(final String id,
                       final Map<Integer, TransactionInput> inputs,
                       final Map<Integer, TransactionOutput> outputs) {
        this.id = id;
        this.inputs = inputs;
        this.outputs = outputs;
    }

    public String getId() {
        return id;
    }

    public Map<Integer, TransactionInput> getInputs() {
        if (inputs == null) {
            inputs = new HashMap<>();
        }

        return inputs;
    }

    public Map<Integer, TransactionOutput> getOutputs() {
        return outputs;
    }
}
