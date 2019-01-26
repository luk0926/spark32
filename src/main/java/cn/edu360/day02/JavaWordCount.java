package cn.edu360.day02;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

/**
 * Created by dell on 2018/6/3.
 */
public class JavaWordCount {
    public static void main(String[] args) {
        if(args.length!=2){
            System.out.println("Usage:cn.edu360.day02.JavaWordCount<host><port>");
            System.exit(1);
        }

        SparkConf conf = new SparkConf();
        JavaSparkContext sc = new JavaSparkContext();

        //读取数据
        JavaRDD<String> file = sc.textFile(args[0]);
        //切分  输入类型，输出类型
        JavaRDD<String> lines = file.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" ")).iterator();
            }
        });

        //组装
        JavaPairRDD<String, Integer> wordWithOne = lines.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<>(word, 1);
            }
        });

        //分组聚合
        JavaPairRDD<String, Integer> result = wordWithOne.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        //排序
        JavaPairRDD<Integer, String> swapedResult = result.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> tp) throws Exception {
                return tp.swap();
            }
        });

        JavaPairRDD<Integer, String> sortedResult = swapedResult.sortByKey(false);

        JavaPairRDD<String, Integer> finalResult = sortedResult.mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<Integer, String> tp) throws Exception {
                return tp.swap();
            }
        });

        //写文件
        finalResult.saveAsTextFile(args[1]);

        //释放资源
        sc.stop();
    }
}
