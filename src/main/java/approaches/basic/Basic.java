package approaches.basic;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;
import scala.Tuple3;
import utils.StringSimilarity;

public class Basic {

	public static final Double LIMIAR = 0.7;
	
	public static void main(String[] args) {

		if (args.length < 3) {
			System.err.println("Please provide the input file full path as argument, the number of partitions and the output path");
			System.exit(0);
		}
		String inputFile = args[0];
		int numPartitions = Integer.valueOf(args[1]);
		String outputPath = args[2];
				
//		SparkConf conf = new SparkConf().setAppName("Basic").setMaster("local");
		SparkConf conf = new SparkConf().setAppName("Basic");
		JavaSparkContext context = new JavaSparkContext(conf);

		JavaRDD<String> file = context.textFile(inputFile, numPartitions);
		JavaRDD<String> entities = file.flatMap(new FlatMapFunction<String, String>() {

			@Override
			public Iterator<String> call(String s) throws Exception {
				return Arrays.asList(s.split("\n")).iterator();
			}
		});

		JavaPairRDD<String, String> key_word_pair = entities.mapToPair(new PairFunction<String, String, String>() {

			@Override
			public Tuple2<String, String> call(String s) throws Exception {
				return new Tuple2<String, String>(getBlockingKey(s), s);
			}
			
			private  String getBlockingKey(String entity) {
				if (entity.length() < 3){
					return ">>>";
				}
				return entity.toLowerCase().substring(0, 3);
			}
		});


		JavaPairRDD<String, Iterable<String>> group_by_key = key_word_pair.groupByKey(numPartitions);
				
		JavaRDD<Tuple3<String, String, Double>> similarity = group_by_key.flatMap(new FlatMapFunction<Tuple2<String,Iterable<String>>, Tuple3<String, String, Double>>() {

			@Override
			public Iterator<Tuple3<String, String, Double>> call(Tuple2<String, Iterable<String>> tuple) throws Exception {
				
				List<Tuple3<String, String, Double>> list = new ArrayList<Tuple3<String, String, Double>>();				
				
				Iterable<String> words =  tuple._2;
				
				int count = 0;
				Double similarity;
				String w2;
				String w1;
				for (Iterator<String> iterator = words.iterator(); iterator.hasNext();) {		
					w1 = iterator.next();
					count++;	
					int it = 0;
					for (Iterator<String> iterator2 = words.iterator(); iterator2.hasNext();) {
						for (int i = it; i < count; i++) {
							it++;
							iterator2.next();
						}
						if (iterator2.hasNext()) {
							w2 =  iterator2.next();
							similarity = StringSimilarity.similarity(w1, w2);
							if (similarity >= LIMIAR) {
								list.add(new Tuple3<String, String, Double>(w1, w2, similarity));
							}
						}
					}
					
				}				
				return list.iterator();
			}
		});
		
		similarity.saveAsTextFile(outputPath);
		
		context.stop();
		context.close();
		System.out.println("DONE!");
	}

}
