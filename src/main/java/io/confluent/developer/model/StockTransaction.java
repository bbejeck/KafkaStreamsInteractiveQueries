package io.confluent.developer.model;

public class StockTransaction {
    private String symbol;
    private boolean buy;
    private double amount;

    public StockTransaction() {}
    public StockTransaction(String symbol, boolean buy, double amount) {
        this.symbol = symbol;
        this.buy = buy;
        this.amount = amount;
    }

    public void setSymbol(String symbol) {
        this.symbol = symbol;
    }

    public void setBuy(boolean buy) {
        this.buy = buy;
    }

    public void setAmount(double amount) {
        this.amount = amount;
    }

    public String getSymbol() {
        return symbol;
    }

    public boolean buy() {
        return buy;
    }

    public double getAmount() {
        return amount;
    }


    public static final class StockTransactionBuilder {
        private String symbol;
        private boolean isBuy;
        private double amount;

        private StockTransactionBuilder() {
        }

        public static StockTransactionBuilder builder() {
            return new StockTransactionBuilder();
        }

        public StockTransactionBuilder withSymbol(String symbol) {
            this.symbol = symbol;
            return this;
        }

        public StockTransactionBuilder withBuy(boolean isBuy) {
            this.isBuy = isBuy;
            return this;
        }

        public StockTransactionBuilder withAmount(double amount) {
            this.amount = amount;
            return this;
        }

        public StockTransaction build() {
            return new StockTransaction(symbol, isBuy, amount);
        }
    }
}
