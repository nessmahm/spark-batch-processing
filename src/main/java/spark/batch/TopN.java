package spark.batch;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;


public class TopN {

    public static void main(String[] args) {

        if (args.length < 1) {
            System.err.println("Usage: SparkKafkaTopNBrands Hbase <bootstrap-servers> <subscribe-topics> <group-id>");
            System.exit(1);
        }
        int topN = 10;

        new TopN().run(args[0], args[1],topN);

    }

    public void run(String inputFilePath, String outputDir, int topN) {
        String hbaseTableName = "top_brands";
        String columnFamily = "stats";

        SparkConf conf = new SparkConf()
                .setAppName(TopN.class.getName());
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> textFile = sc.textFile(inputFilePath);

        //new TopN().run(args[0], args[1],topN,hbaseTableName,columnFamily);
        // Count occurrences of each brand from the 5th column
        JavaPairRDD<String, Integer> counts = textFile
                .flatMapToPair(line -> {
                    // Split the line into columns using comma as the delimiter
                    String[] columns = line.split(",");
                    // Extract the brand from the 5th column (index 6)
                    String brand = columns[5];
                    // Return a pair of (brand, 1) for counting
                    return Arrays.asList(new Tuple2<>(brand, 1)).iterator();
                })
                .reduceByKey(Integer::sum);



        // Sort by count in descending order and take the top N brands without swapping the key and value
        List<Tuple2<String, Integer>> topNBrands = counts
                .mapToPair(tuple -> new Tuple2<>(tuple._2, tuple._1)) // Swap key and value for sorting
                .sortByKey(false) // Sort by count in descending order
                .map(tuple -> new Tuple2<>(tuple._2, tuple._1)) // Revert the swap to maintain original types
                .take(topN); // Replace 10 with your desired top N count

        // Save top N brands to output directory or process as needed
        saveToHBase(topNBrands,hbaseTableName,columnFamily);
        JavaRDD<Tuple2<String, Integer>> topNBrandsRDD = sc.parallelize(topNBrands);
        topNBrandsRDD.saveAsTextFile(outputDir);



        // Close the Spark context
        sc.close();
    }
    private void saveToHBase(List<Tuple2<String, Integer>> topNBrands, String hbaseTableName,String columnFamily)
    {
        Configuration hbaseConfig = HBaseConfiguration.create();
        // Create HBase configuration
        try (Connection connection = ConnectionFactory.createConnection(hbaseConfig))
              {
                  TableName tableName = TableName.valueOf(hbaseTableName);
                  Table table = connection.getTable(TableName.valueOf(hbaseTableName));

                  // Iterate through top N brands and save each to HBase
                  for (Tuple2<String, Integer> brand : topNBrands) {
                      // Create a new row in HBase with the branecho "# spark-batch-processing" >> README.mdd and count
                      String brand_name = brand._1;
                      Integer brand_count = brand._2;
                      if (brand_name.isEmpty()) {
                          // Replace empty key with "unknown"
                          brand_name = "unknown";
                      }
                      Put put = new Put(Bytes.toBytes(brand_name));

                      put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("count"), Bytes.toBytes(Long.toString(brand_count)));
                      put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("brand"), Bytes.toBytes(brand_name));

                      // Write the new data directly to HBase, replacing the existing result
                      table.put(put);

                      System.out.println("Reading data...");
                      Get g = new Get(Bytes.toBytes(brand_name));
                      Result r = table.get(g);
                      System.out.println(Bytes.toString(r.getValue(Bytes.toBytes(columnFamily), Bytes.toBytes("brand"))));
                      // Log success message for each updated brand

                  }
        }

         catch (IOException e) {
             System.out.println("Error saving to HBase: "+ e.getMessage());
        }
    }
}
