import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class SparkMain2 {
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
                .option("inferSchema", true).option("delimiter", "|").csv("trades_100M.csv").repartition(col(COL_CURRENCY));

        Dataset<Row> dsDelta = dsTrades.filter(col(COL_MEASURE).equalTo(lit(MEASURE_TYPE_DELTA)));
        Dataset<Row> dsCurvature = dsTrades.filter(col(COL_MEASURE).equalTo(lit(MEASURE_TYPE_CURVATURE)));

        Dataset<Row> dsSumDelta = dsDelta.groupBy(COL_TRADE_ID, COL_CURRENCY)
                .agg(bround(sum(col(COL_AMOUNT)), 7))
                .withColumnRenamed(COL_TRADE_ID, COL_DELTA_TRADE_ID)
                .withColumnRenamed(COL_CURRENCY, COL_DELTA_CURRENCY);
        dsSumDelta.cache();

        Dataset<Row> dsCurvatureWithSumDelta = dsCurvature.join(dsSumDelta, col(COL_DELTA_TRADE_ID).equalTo(col(COL_TRADE_ID)).and(col(COL_DELTA_CURRENCY).equalTo(col(COL_CURRENCY))));

        System.out.println(dsSumDelta.count());
        System.out.println(dsCurvatureWithSumDelta.count());

       // dsTrades.show(1000);
    }
}
