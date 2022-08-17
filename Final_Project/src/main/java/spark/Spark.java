package spark;

import org.apache.kafka.common.errors.TimeoutException;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.StructType;

public class Spark {
    public static void main(String[] args) throws TimeoutException, StreamingQueryException {
        SparkSession spark = SparkSession
                .builder()
                .appName("Streaming with Kafka")
                //.master("local")
                .config("spark.dynamicAllocation.enabled","false")
                .getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");
        ReadStream(spark);
    }

    private static void ReadStream(SparkSession spark) throws TimeoutException, StreamingQueryException {
        StructType s = new StructType()
                .add("id", "STRING")
                .add("name", "STRING")
                .add("author_name", "STRING")
                .add("inventory_status", "STRING")
                .add("short_description", "STRING")
                .add("original_price", "INT")
                .add("discount", "INT")
                .add("price", "INT")
                .add("discount_rate", "INT")
                .add("rating_average", "FLOAT")
                .add("review_count", "INT")
                .add()
    }
}
