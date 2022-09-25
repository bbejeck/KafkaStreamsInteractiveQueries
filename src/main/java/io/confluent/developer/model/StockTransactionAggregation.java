package io.confluent.developer.model;

public class StockTransactionAggregation {
    private String symbol;
    private double buys = 0.0;
    private double sells = 0.0;

    public StockTransactionAggregation () {}

    public StockTransactionAggregation update(StockTransaction transaction) {
        if (symbol == null) {
            symbol = transaction.getSymbol();
        }
        if (transaction.buy()) {
             buys += transaction.getAmount();
        } else {
            sells += transaction.getAmount();
        }
        return this;
    }
    public String getSymbol() {
        return symbol;
    }

    public double getBuys() {
        return buys;
    }

    public double getSells() {
        return sells;
    }
}
