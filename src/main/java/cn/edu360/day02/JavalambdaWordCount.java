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
 * Created by dell on 2018/6/3.
 */
public class JavalambdaWordCount {
    public static void main(String[] args) {
        if(args.length!=2){
            System.out.println("Usage:cn.edu360.day02.JavalambdaWordCount<input><outport>");
            System.exit(1);
        }

        SparkConf conf = new SparkConf();
        JavaSparkContext sc = new JavaSparkContext(conf);
        
        //读取文件
        JavaRDD<String> file = sc.textFile(args[0]);

        //切割并压平
        JavaRDD<String> lines = file.flatMap(t -> Arrays.asList(t.split(" ")).iterator());

        //组装
        JavaPairRDD<String, Integer> wordWithOne = lines.mapToPair(t -> new Tuple2<String, Integer>(t, 1));

        //分组聚合
        JavaPairRDD<String, Integer> result = wordWithOne.reduceByKey((a, b) -> a + b);

        //排序 先K,V互换
        JavaPairRDD<Integer, String> swapResult = result.mapToPair(t -> t.swap());
        //再排序
        JavaPairRDD<Integer, String> sortedResult = swapResult.sortByKey(false);
        JavaPairRDD<String, Integer> finalResult = sortedResult.mapToPair(t -> t.swap());

        //写入文件
        finalResult.saveAsTextFile(args[1]);

        //释放资源
        sc.stop();

    }
}
