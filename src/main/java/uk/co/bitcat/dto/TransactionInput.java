package uk.co.bitcat.dto;

public class TransactionInput {

    private String txId;
    private int utxoIndex;
    private String address;

    public TransactionInput() {};

    public TransactionInput(final String txId, final int utxoIndex, final String address) {
        this.txId = txId;
        this.utxoIndex = utxoIndex;
        this.address = address;
    }

    public String getTxId() {
        return txId;
    }

    public int getUtxoIndex() {
        return utxoIndex;
    }

    public String getAddress() {
        return address;
    }
}
