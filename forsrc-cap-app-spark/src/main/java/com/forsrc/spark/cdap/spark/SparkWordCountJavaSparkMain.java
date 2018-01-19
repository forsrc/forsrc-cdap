package com.forsrc.spark.cdap.spark;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.commons.io.Charsets;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import co.cask.cdap.api.spark.JavaSparkExecutionContext;
import co.cask.cdap.api.spark.JavaSparkMain;
import scala.Tuple2;

public class SparkWordCountJavaSparkMain implements JavaSparkMain {

    private static final long serialVersionUID = 1L;
    private static final Pattern SPACE = Pattern.compile("\\s+");

    @Override
    public void run(JavaSparkExecutionContext sec) throws Exception {

        JavaPairRDD<Long, String> streamWords = sec.fromStream("sparkWordCountStream", String.class);

        JavaRDD<String> words = streamWords.flatMap(new FlatMapFunction<Tuple2<Long,String>, String>() {
            private static final long serialVersionUID = 1L;
            @Override
            public Iterator<String> call(Tuple2<Long, String> t) throws Exception {
                return Arrays.asList(SPACE.split(t._2)).iterator();
            }
        });
        //JavaRDD<String> words = streamWords.flatMap(word -> Arrays.asList(SPACE.split(word._2)).iterator());
        JavaPairRDD<String, Integer> counts = words.mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((Integer i1, Integer i2) -> (i1 + i2));
//        List<Tuple2<String, Integer>> output = counts.collect();
//        output.forEach(item -> {
//            
//        });

//        JavaPairRDD<byte[], Integer> countRaw = counts.mapToPair(new PairFunction<Tuple2<String, Integer>, byte[], Integer>() {
//            private static final long serialVersionUID = 4302076336292178530L;
//            @Override
//            public Tuple2<byte[], Integer> call(Tuple2<String, Integer> t) throws Exception {
//                return new Tuple2<>(t._1().getBytes(Charsets.UTF_8), t._2());
//            }
//        });
        JavaPairRDD<byte[], Integer> countRaw = counts.mapToPair(t -> new Tuple2<>(t._1().getBytes(Charsets.UTF_8), t._2()));
        sec.saveAsDataset(countRaw, SparkWordCountConfig.KEY_VALUE_DATASET);
    }

}
