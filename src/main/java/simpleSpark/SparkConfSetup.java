package simpleSpark;

import com.datastax.spark.connector.cql.CassandraConnector;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;

public interface SparkConfSetup {

    //DSE Host is the IP of the Spark Master started by DSE
    String DSE_HOST = "172.31.9.194";

    //Driver Host is the ip of the application running the spark application
    //String DRIVER_HOST = "127.0.0.1";

    static public SparkConf getSparkConf() {

        return new SparkConf()
                .setAppName("SimpleSpark")
                .set("spark.cassandra.connection.host",DSE_HOST);
                //.set("spark.driver.host", DRIVER_HOST)
                //.setMaster("spark://" + DSE_HOST + ":7077");
    }

    static public JavaSparkContext getJavaSparkContext() {
        SparkContext sparkContext = new SparkContext(getSparkConf());
        return new JavaSparkContext(sparkContext);
    }

    static public CassandraConnector getCassandraConnector() {
        return CassandraConnector.apply((getSparkConf()));
    }
}
