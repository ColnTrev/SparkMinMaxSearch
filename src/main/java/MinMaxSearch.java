import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.*;

/**
 * Created by colntrev on 2/26/18.
 */
public class MinMaxSearch {
    public static void main(String[] args){

        String inputFile = args[0];
        final int N = 10;
        SparkConf conf = new SparkConf().setAppName("MinMax Search");
        JavaSparkContext context = new JavaSparkContext(conf);
        JavaRDD<String> input = context.textFile(inputFile,1);

        JavaPairRDD<String, Integer> pair = input.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String s) throws Exception {
                String[] tok = s.split(" ");
                return new Tuple2<String, Integer>(tok[0], Integer.parseInt(tok[1]));
            }
        });

        JavaRDD<SortedMap<Integer, String>> partition = pair.mapPartitions(new FlatMapFunction<Iterator<Tuple2<String, Integer>>, SortedMap<Integer, String>>() {
            public Iterator<SortedMap<Integer, String>> call(Iterator<Tuple2<String, Integer>> tuple2Iterator) throws Exception {
                SortedMap<Integer, String> topK = new TreeMap<Integer, String>();
                while(tuple2Iterator.hasNext()){
                    Tuple2<String, Integer> t = tuple2Iterator.next();
                    topK.put(t._2(),t._1());
                    if(topK.size() > N) {
                        topK.remove(topK.firstKey());
                    }
                }
                return Collections.singletonList(topK).iterator(); // need return to be immutable
            }
        });

        SortedMap<Integer, String> topK = new TreeMap<Integer, String>();
        List<SortedMap<Integer,String>> res = partition.collect();

        for(SortedMap<Integer, String> localRes : res){
            for(Map.Entry<Integer,String> entry : localRes.entrySet()){
                topK.put(entry.getKey(), entry.getValue());
                if(topK.size() > N){
                    topK.remove(topK.firstKey());
                }
            }
        }

        for(Map.Entry<Integer,String> entry : topK.entrySet()){
            System.out.println(entry.getKey() + " " + entry.getValue());
        }
    }
}
