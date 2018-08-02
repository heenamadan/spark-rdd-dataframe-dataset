package example_java.dataset;

import example_java.common.JavaData;
import example_java.common.Person;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SQLContext;

import java.util.List;

public class JavaDatasetExample {

    public static void main(String[] args) throws Exception {

        SparkConf sparkConf = new SparkConf()
                .setAppName("Example")
                .setMaster("local[*]");

        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        SQLContext sqlContext = new SQLContext(sc);

        List<Person> data = JavaData.sampleData();

        Dataset<Person> dataset = sqlContext.createDataset(data, Encoders.bean(Person.class));

        Dataset<Person> below21 = dataset.filter((FilterFunction<Person>) person -> (person.getAge() < 21));

        below21.foreach((ForeachFunction<Person>) person -> System.out.println(person));

    }
}
