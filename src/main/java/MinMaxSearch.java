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
        if(args.length < 3){
            System.out.println("<input file> <N> <size>");
            System.exit(-1);
        }
        String inputFile = args[0];
        int N = Integer.parseInt(args[1]);
        int size = Integer.parseInt(args[2]);
        boolean show = args.length == 4? Boolean.parseBoolean(args[3]) : false;
        SparkConf conf = new SparkConf().setAppName("MinMax Search");
        JavaSparkContext context = new JavaSparkContext(conf);


        JavaRDD<String> input = context.textFile(inputFile,1);

        long startTime = System.currentTimeMillis();
        JavaPairRDD<Double, Double> pair = input.mapToPair(s -> {
                String[] tok = s.split(" ");
                return new Tuple2<>(Double.parseDouble(tok[0]), Double.parseDouble(tok[1]));
        });

        JavaRDD<SortedMap<Double, String>> partition = pair.mapPartitions(tuple2Iterator -> {
                SortedMap<Double, String> topK = new TreeMap<>();
                double lower = -5.12;
                double upper = 5.12;
                int A = 10;
                while(tuple2Iterator.hasNext()){
                    Tuple2<Double, Double> t = tuple2Iterator.next();
                    double x = t._1() / size * (upper - lower) + lower;
                    double y = t._2() /size * (upper - lower) + lower;
                    double rast = 2 * A + (x*x - A * Math.cos(2 * Math.PI * x)) + (y*y - A * Math.cos(2 * Math.PI * y));
                    topK.put(rast,t.toString());
                    if(topK.size() > N) {
                        topK.remove(topK.lastKey());
                    }
                }
                return Collections.singletonList(topK).iterator();
        });

        SortedMap<Double, String> topK = new TreeMap<>();
        List<SortedMap<Double,String>> res = partition.collect();

        for(SortedMap<Double, String> localRes : res){
            for(Map.Entry<Double,String> entry : localRes.entrySet()){
                topK.put(entry.getKey(), entry.getValue());
                if(topK.size() > N){
                    topK.remove(topK.lastKey());
                }
            }
        }
        long endTime = System.currentTimeMillis();
        if(show) {
            for (Map.Entry<Double, String> entry : topK.entrySet()) {
                System.out.println(entry.getKey() + " " + entry.getValue());
            }
        }
        System.out.println("Elapsed Time: " + (endTime - startTime));
    }
}
