package uk.co.bitcat.dto;

public class TransactionOutput {

    private int amount;
    private String address;

    public TransactionOutput() {};

    public TransactionOutput(final int amount, final String address) {
        this.amount = amount;
        this.address = address;
    }

    public int getAmount() {
        return amount;
    }

    public String getAddress() {
        return address;
    }

    public void setAmount(int amount) {
        this.amount = amount;
    }

    public void setAddress(String address) {
        this.address = address;
    }
}
