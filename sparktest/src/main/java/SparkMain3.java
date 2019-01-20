import org.apache.spark.api.java.function.FlatMapGroupsFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.MapGroupsFunction;
import org.apache.spark.sql.*;

import java.util.*;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.apache.spark.sql.functions.*;

public class SparkMain3 {
    private static final SparkSession spark = SparkSession.builder()
            .appName("TestsCCA.com - Spark")
            .master("local[*]")
            .getOrCreate();

    public static final String COL_TRADE_ID = "tradeid";
    public static final String COL_CURRENCY = "currency";
    public static final String COL_MEASURE = "measure";
    public static final String COL_AMOUNT = "amount";
    public static final String COL_PRODUCT_ID = "productid";

    public static final String COL_DELTA_TRADE_ID = "deltatradeid";
    public static final String COL_DELTA_CURRENCY = "deltacurrency";
    public static final String COL_DELTA_SUM = "deltasum";

    public static final String MEASURE_TYPE_DELTA = "DELTA";
    public static final String MEASURE_TYPE_CURVATURE = "CURVATURE";


    public static void main(String[] args) {
        Dataset<Row> dsTrades = spark.read().option("header", true)
                .option("inferSchema", true).option("delimiter", "|").csv("trades_10000.csv").repartition(col(COL_CURRENCY));


        Dataset<Row> resultDs = dsTrades.groupByKey((MapFunction<Row, TradeKey>) r -> new TradeKey(r.getAs(COL_TRADE_ID), r.getAs(COL_CURRENCY)), Encoders.bean(TradeKey.class))
                .flatMapGroups((FlatMapGroupsFunction<TradeKey, Row, Row>) (tradeKey, iterator) -> {
                    Map<String, List<Row>> map = asStream(iterator).filter(optionnalityFilterAlmost0()).collect(Collectors.groupingBy(r -> r.getAs(COL_MEASURE)));
                    return map.containsKey("VEGA")?map.get("CURVATURE").iterator():Collections.emptyIterator();
                }, dsTrades.exprEnc());

        resultDs.show();

    }

    private static Predicate<Row> optionnalityFilterPresent() {
        return (Row r) -> {
            return true;
        };
    }

    private static Predicate<Row> optionnalityFilter0() {
        return (Row r) -> {
            return !"VEGA".equals(r.getAs(COL_MEASURE)) || Math.abs(r.getDouble(r.fieldIndex(COL_AMOUNT))) != 0d;
        };
    }

    private static Predicate<Row> optionnalityFilterAlmost0() {
        return (Row r) -> {
            return !"VEGA".equals(r.getAs(COL_MEASURE)) || Math.abs(r.getDouble(r.fieldIndex(COL_AMOUNT))) >= 0.0001d;
        };
    }


    public static class TradeKey {

        public TradeKey() {

        }


        public TradeKey(int tradeId, String currency) {
            this.setCurrency(currency);
            this.setTradeId(tradeId);
        }

        private int tradeId;
        private String currency;


        public int getTradeId() {
            return tradeId;
        }

        public void setTradeId(int tradeId) {
            this.tradeId = tradeId;
        }

        public String getCurrency() {
            return currency;
        }

        public void setCurrency(String currency) {
            this.currency = currency;
        }
    }

    public static <T> Stream<T> asStream(Iterator<T> sourceIterator) {
        return asStream(sourceIterator, false);
    }

    public static <T> Stream<T> asStream(Iterator<T> sourceIterator, boolean parallel) {
        Iterable<T> iterable = () -> sourceIterator;
        return StreamSupport.stream(iterable.spliterator(), parallel);
    }
}
