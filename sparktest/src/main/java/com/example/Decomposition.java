package com.example;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class Decomposition {

    public static void main(String[] args) {

        SparkConf conf = new
                SparkConf().setMaster("local").setAppName("Decompo");

        SparkSession spark = SparkSession
                .builder()
                .appName("Application Name")
                .config(conf)
                .config("spark.sql.warehouse.dir", ".")
                .getOrCreate();

        Dataset<Row> inputData = spark.read()
                .option("delimiter", "|")
                .option("inferSchema", "true")
                .option("header", "true").csv("matrix.csv");

        Encoder<UnderlyingComponent> ulEncoder = Encoders.bean(UnderlyingComponent.class);
        Dataset<UnderlyingComponent> ulDecompo = inputData.as(ulEncoder);
        List<UnderlyingComponent> listULC = ulDecompo.collectAsList().stream().collect(Collectors.toList());

        // build the tree of underlyings
        Map<String, Product> productMap = buildProductsMap(listULC);
        linkProducts(productMap, listULC);

        // compute the weight of each share for each underlying
        List<String> underlyings = listULC.stream().map(UnderlyingComponent::getUnderlying).distinct().collect(Collectors.toList());
        Map<String, Map<String, Share>> shares = new HashMap<>();
        underlyings.stream().forEach(ul -> shares.put(ul, transparise(productMap.get(ul), 1d, new HashMap())));

        List<Row> flattenDecompo = shares.entrySet().stream().flatMap(entry->entry.getValue().values().stream().map(share->RowFactory.create(entry.getKey(),share.name,share.shareType,share.weight))).collect(Collectors.toList());
        Dataset<Row> ds = spark.createDataFrame(flattenDecompo, ulEncoder.schema());
        ds.show();
    }

    private static Map<String, Share> transparise(Product product, Double parentWeight, Map<String, Share> shares) {
        for (ProductNode child : product.children) {
            if (child.product.children.isEmpty()) {
                if (shares.containsKey(child.product.name)) {
                    shares.get(child.product.name).weight += child.weight * parentWeight;
                } else {
                    shares.put(child.product.name, new Share(child.product.name, child.product.shareType, child.weight * parentWeight));
                }
            } else {
                shares = transparise(child.product, child.weight * parentWeight, shares);
            }
        }
        return shares;
    }


    private static Map<String, Product> buildProductsMap(List<UnderlyingComponent> listULC) {
        Map<String, Product> mapProducts = new HashMap();
        listULC.stream().forEach(ulc -> {
            mapProducts.putIfAbsent(ulc.getShare(), new Product(ulc.getShare(), ulc.getShareType()));
        });
        listULC.stream().forEach(ulc -> {
            mapProducts.putIfAbsent(ulc.getUnderlying(), new Product(ulc.getShare(), null));
        });
        return mapProducts;
    }

    private static void linkProducts(Map<String, Product> productMap, List<UnderlyingComponent> listULC) {
        listULC.stream().forEach(ulc -> productMap.get(ulc.getUnderlying()).addChild(new ProductNode(productMap.get(ulc.getShare()), ulc.getWeight())));

    }

    static class Share {
        String name;
        String shareType;
        Double weight;

        public Share(String name, String shareType, Double weight) {
            this.name = name;
            this.shareType = shareType;
            this.weight = weight;
        }
    }


    static class Product {
        String name;
        String shareType;
        List<ProductNode> children = new ArrayList<>();

        public Product(String name, String shareType) {
            this.name = name;
            this.shareType = shareType;
        }

        void addChild(ProductNode pn) {
            this.children.add(pn);
        }
    }

    static class ProductNode {
        public ProductNode(Product product, Double weight) {
            this.product = product;
            this.weight = weight;
        }

        Product product;
        Double weight;
    }

}
