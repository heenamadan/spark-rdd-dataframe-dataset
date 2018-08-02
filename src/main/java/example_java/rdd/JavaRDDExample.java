package example_java.rdd;

import example_java.common.FileData;
import example_java.common.Person;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class JavaRDDExample {

    public static void main(String[] args) throws Exception {

        SparkConf sparkConf = new SparkConf()
                .setAppName("Example")
                .setMaster("local[*]");

        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        JavaRDD<Person> rdd = sc.parallelize(FileData.sampleData());

        rdd.filter(p -> p.getAge() < 18)
                .collect()
                .forEach(System.out::println);

    }
}
