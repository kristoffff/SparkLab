import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.Iterator;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.split;
import static org.apache.spark.sql.functions.explode;
import static org.apache.spark.sql.functions.lit;

public class SparkMain {
    private static final SparkSession spark = SparkSession.builder()
            .appName("TestsCCA.com - Spark")
            .master("local[*]")
            .getOrCreate();

    public static void main1(String[] args) {

        //increment10(100);

        Dataset<Row> dsDeals = spark.read().option("header", true)
                .option("inferSchema", true).option("delimiter", "|").csv("deals.csv");
        Dataset<Row> dsProducts = spark.read().option("header", true)
                .option("inferSchema", true).option("delimiter", "|").csv("products.csv");
        Dataset<Row> dsProducts2 = spark.read().option("header", true)
                .option("inferSchema", true).option("delimiter", "|").csv("products.csv");


        if (!Arrays.asList(dsProducts.columns()).containsAll(Arrays.asList("ul_type", "ul_id"))) {
            if (Arrays.asList(dsProducts.columns()).contains("ul_type")) {
                dsProducts = dsProducts.drop("ul_type");
            }
            if (Arrays.asList(dsProducts.columns()).contains("ul_id")) {
                dsProducts = dsProducts.drop("ul_id");
            }
            dsProducts = dsProducts.withColumn("ul_id", lit(null));
            dsProducts = dsProducts.withColumn("ul_type", lit(null));
        }

        Dataset<Row> dsUnderlying = dsProducts.select("id", "product_name", "type")
                .withColumnRenamed("id", "udl_id")
                .withColumnRenamed("product_name", "ul_name")
                .withColumnRenamed("type", "ul_type");


        Dataset<Row> result = dsDeals.join(dsProducts, col("product_id").equalTo(dsProducts.col("id"))).filter(col("ul_type").equalTo("ACTION"))
                .join(dsUnderlying, col("ul_id").equalTo(dsUnderlying.col("udl_id")), "left");


        Dataset<Row> indexes = dsProducts.filter(col("type").equalTo("INDEX"));
        dsProducts.repartition();
        Dataset<Row> indexComposition = indexes.withColumn("index_underlying", explode(split(indexes.col("elements"), ","))).drop(col("elements"));
        indexes.show();
        indexComposition.show();
        /*Dataset<Row> index = dsDeals.join(dsProducts, col("product_id").equalTo(dsProducts.col("id"))).filter(col("type").equalTo("INDEX"))
                .join(dsUnderlying, col("index").cast(DataTypes.StringType).equalTo(dsUnderlying.col("udl_id")), "left");


        //result.explain();
        index.show();*/


        Dataset<Row> dealsOnIndex = dsDeals.join(indexComposition, dsDeals.col("product_id").equalTo(indexes.col("id")));
        dealsOnIndex.show();
        Dataset<Row> dealsOnIndexWithIndexDetail = dealsOnIndex.join(dsProducts2, dealsOnIndex.col("index_underlying").equalTo(dsProducts2.col("id")), "left");
        dealsOnIndexWithIndexDetail.show();
        //.join(indexComposition, indexes.col("id").equalTo(indexComposition.col("index_underlying")));
//        /dealWithIndexComposition.show();

        Dataset<Integer> toto = dsProducts.flatMap(new FlatMapFunction<Row, Integer>() {

            public Iterator<Integer> call(Row row) throws Exception {
                return Arrays.asList(row.getString(row.fieldIndex("elements")).split(",")).stream().map(s -> Integer.valueOf(s)).collect(Collectors.<Integer>toList()).iterator();
            }
        }, Encoders.INT());

        // ataset<Row> dsProductsInIndex = dsDeals.filter(dsDeals.col("product_id").isin(split(dsProducts.col("elements"), ",")));
        toto.show();



    }
    public static void  main(String[]  args) {
        int input = 100;
        for (int i=0;i<10;i++) {
            increment(input);
        }
        System.out.println(input);
    }

    public static void increment(int input) {
       input++;
    }
}
