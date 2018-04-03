package uk.co.bitcat.dto;

public class TransactionInput {

    private String txId;
    private int utxoIndex;
    private int amount;
    private String address;

    public TransactionInput() {};

    public TransactionInput(final String txId, final int utxoIndex, final int amount, final String address) {
        this.txId = txId;
        this.utxoIndex = utxoIndex;
        this.amount = amount;
        this.address = address;
    }

    public String getTxId() {
        return txId;
    }

    public int getUtxoIndex() {
        return utxoIndex;
    }

    public int getAmount() {
        return amount;
    }

    public String getAddress() {
        return address;
    }

    public void setTxId(String txId) {
        this.txId = txId;
    }

    public void setUtxoIndex(int utxoIndex) {
        this.utxoIndex = utxoIndex;
    }

    public void setAmount(int amount) {
        this.amount = amount;
    }

    public void setAddress(String address) {
        this.address = address;
    }
}
