package cn.edu360.day02;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import scala.Tuple2;

import java.util.Arrays;

/**
 * Created by dell on 2018/6/4.
 */
public class JavaLambdaWordCount2 {
    public static void main(String[] args) {
        if(args.length!=2){
            System.out.println("Usage:cn.edu360.day02.JavaLambdaWordCount2<input><output>");
            System.exit(1);
        }

        SparkConf conf = new SparkConf();
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> file = sc.textFile(args[0]);

        JavaRDD<String> split = file.flatMap(t -> Arrays.asList(t.split(" ")).iterator());

        JavaPairRDD<String, Integer> map = split.mapToPair(t -> new Tuple2<String, Integer>(t, 1));

        JavaPairRDD<String, Integer> result = map.reduceByKey((a, b) -> a + b);

        result.saveAsTextFile(args[1]);
    }
}
