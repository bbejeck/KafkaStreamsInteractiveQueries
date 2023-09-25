package io.confluent.developer.model;

public class StockTransaction {
    private String symbol;
    private boolean buy;
    private double amount;

    private int numberShares;

    public StockTransaction() {}
    public StockTransaction(String symbol,
                            boolean buy,
                            double amount,
                            int numberShares) {
        this.symbol = symbol;
        this.buy = buy;
        this.amount = amount;
        this.numberShares = numberShares;
    }

    public int getNumberShares() {
        return numberShares;
    }

    public void setNumberShares(int numberShares) {
        this.numberShares = numberShares;
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

    public boolean getBuy() {
        return buy;
    }

    public double getAmount() {
        return amount;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof StockTransaction that)) return false;

        if (buy != that.getBuy()) return false;
        if (Double.compare(that.getAmount(), getAmount()) != 0) return false;
        return getSymbol().equals(that.getSymbol());
    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        result = getSymbol().hashCode();
        result = 31 * result + (buy ? 1 : 0);
        temp = Double.doubleToLongBits(getAmount());
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        return result;
    }

    public static final class StockTransactionBuilder {
        private String symbol;
        private boolean isBuy;
        private double amount;

        private int numberShares;

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

        public StockTransactionBuilder withNumberShares(int numberShares) {
            this.numberShares = numberShares;
            return this;
        }

        public StockTransaction build() {
            return new StockTransaction(symbol, isBuy, amount, numberShares);
        }
    }
}
