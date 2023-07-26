package io.confluent.developer.model;

import java.util.Objects;

public class StockTransactionAggregation {
    private String symbol;
    private double buys = 0.0;
    private double sells = 0.0;

    public StockTransactionAggregation () {}

    public StockTransactionAggregation(String symbol, double buys, double sells) {
        this.symbol = symbol;
        this.buys = buys;
        this.sells = sells;
    }

    public StockTransactionAggregation update(StockTransaction transaction) {
        if (symbol == null) {
            symbol = transaction.getSymbol();
        }
        if (transaction.getBuy()) {
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof StockTransactionAggregation)) return false;
        StockTransactionAggregation that = (StockTransactionAggregation) o;
        return Double.compare(that.getBuys(), getBuys()) == 0 && Double.compare(that.getSells(), getSells()) == 0 && getSymbol().equals(that.getSymbol());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getSymbol(), getBuys(), getSells());
    }

    @Override
    public String toString() {
        return "StockTransactionAggregation{" +
                "symbol='" + symbol + '\'' +
                ", buys=" + buys +
                ", sells=" + sells +
                '}';
    }
}
