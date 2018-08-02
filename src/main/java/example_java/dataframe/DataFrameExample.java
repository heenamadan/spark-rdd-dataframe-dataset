package example_java.dataframe;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;



public class DataFrameExample {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("DataFrameExample").setMaster("local[*]");
        JavaSparkContext jsc = new 	JavaSparkContext(conf);

        //Create the sql context
        SQLContext sqlContext =  new SQLContext(jsc);

        String schemaString = "name,empid,salary";

        List<StructField> fieldList = new ArrayList<StructField>();

        //Create 3 objects for 3 columns, please note that salary is an integer
        fieldList.add(DataTypes.createStructField("name",DataTypes.StringType,true));
        fieldList.add(DataTypes.createStructField("empid",DataTypes.StringType,true));
        fieldList.add(DataTypes.createStructField("salary",DataTypes.IntegerType,true));

        //Now create a StructType object from the list
        StructType schema = DataTypes.createStructType(fieldList);

        //Load the file which contains the data into an RDD
        JavaRDD<String> rdd = jsc.textFile("src/main/resources/employee.txt");

        //convert the input to a list of row objects
        JavaRDD<Row> rowsRDD = rdd.map(new Function<String,Row>(){

            @Override
            public Row call(String arg0) throws Exception {
                //arg0 contains each line from the rdd , i.e empid,name,salary
                String [] data = arg0.split(",");

                //We make sure that we convert the salary field to integer
                return RowFactory.create(data[0],data[1],Integer.valueOf(data[2]));
            }});

        //Create the dataframe from the rowsRDD and schema
        DataFrame dataFrame = sqlContext.createDataFrame(rowsRDD, schema);
        dataFrame.registerTempTable("employee");

        //Run the sql query to find details of employee who draws the max salary
        DataFrame maxSalDataFrame = sqlContext.sql("Select * from employee order by salary desc limit 1 ");

        maxSalDataFrame.toJavaRDD().foreach(new VoidFunction<Row>(){

            @Override
            public void call(Row arg0) throws Exception {
                System.out.println(" Name is :"+arg0.get(0)+" Emp id is:"+arg0.get(1)+" Salary is:"+arg0.get(2));
            }});
    }
}
