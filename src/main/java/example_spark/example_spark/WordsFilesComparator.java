package example_spark.example_spark;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;
import scala.Tuple3;
import utils.StringSimilarity;

public class WordsFilesComparator {

	private static final Double LIMIAR = 0.7;

	private static final FlatMapFunction<String, String> WORDS_EXTRACTOR = new FlatMapFunction<String, String>() {
		public Iterator<String> call(String s) throws Exception {
			return Arrays.asList(s.split(" ")).iterator();
		}
	};

	private static final PairFunction<Tuple2<String, String>, Tuple2<String, String>, Double> WORDS_SIMILARITY = new PairFunction<Tuple2<String, String>, Tuple2<String, String>, Double>() {

		@Override
		public Tuple2<Tuple2<String, String>, Double> call(Tuple2<String, String> tuple) throws Exception {
			Double similarity = StringSimilarity.similarity(tuple._1, tuple._2);
			return new Tuple2<Tuple2<String, String>, Double>(tuple, similarity);
		}

	};

	public static final Function<Tuple2<Tuple2<String, String>, Double>, Boolean> FILTER_MORE_SIMILAR = new Function<Tuple2<Tuple2<String, String>, Double>, Boolean>() {

		@Override
		public Boolean call(Tuple2<Tuple2<String, String>, Double> tuple) throws Exception {
			return tuple._2 >= LIMIAR;
		}
	};

	public static <U> void main(String[] args) {
		if (args.length < 1) {
			System.err.println("Please provide the input file full path as argument");
			System.exit(0);
		}

		SparkConf conf = new SparkConf().setAppName("com.wordexample.spark.WordCount").setMaster("local");
		JavaSparkContext context = new JavaSparkContext(conf);

		JavaRDD<String> file1 = context.textFile(args[0]).distinct();
		JavaRDD<String> words1 = file1.flatMap(WORDS_EXTRACTOR);
		JavaRDD<String> file2 = context.textFile(args[1]).distinct();
		JavaRDD<String> words2 = file2.flatMap(WORDS_EXTRACTOR);

		JavaPairRDD<String, String> keyBlockFile1 = words1.mapToPair(new PairFunction<String, String, String>() {

			@Override
			public Tuple2<String, String> call(String s) throws Exception {
				return new Tuple2<String, String>(s.toLowerCase().substring(0, 1), s);
			}
		});

		JavaPairRDD<String, String> keyBlockFile2 = words2.mapToPair(new PairFunction<String, String, String>() {

			@Override
			public Tuple2<String, String> call(String s) throws Exception {
				return new Tuple2<String, String>(s.toLowerCase().substring(0, 1), s);
			}
		});

		// Aqui eu tenho a chave e uma tupla que contém as palavras do primeiro
		// arquivo e as palavras do segundo arquivo
		// que tem aquela chave
		JavaPairRDD<String, Tuple2<Iterable<String>, Iterable<String>>> groups = keyBlockFile1.cogroup(keyBlockFile2);

		JavaRDD<Iterable<Tuple3<String, String, Double>>> comparables = groups.map(
				new Function<Tuple2<String, Tuple2<Iterable<String>, Iterable<String>>>, Iterable<Tuple3<String, String, Double>>>() {

					@Override
					public Iterable<Tuple3<String, String, Double>> call(
							Tuple2<String, Tuple2<Iterable<String>, Iterable<String>>> v1) throws Exception {

						List<Tuple3<String, String, Double>> ite = new ArrayList<Tuple3<String, String, Double>>();

						Tuple2<Iterable<String>, Iterable<String>> tuple2 = v1._2;

						for (String palavra1 : tuple2._1) {
							for (String palavra2 : tuple2._2) {
								Double similarity = StringSimilarity.similarity(palavra1, palavra2);
								if (similarity >= LIMIAR) {
									Tuple3<String, String, Double> result = new Tuple3<String, String, Double>(palavra1,
											palavra2, similarity);
									ite.add(result);
								}
							}
						}
						return ite;
					}
				});

		List<Iterable<Tuple3<String, String, Double>>> lista = comparables.collect();

		for (Iterable<Tuple3<String, String, Double>> iterat : lista) {
			for (Tuple3<String, String, Double> tuple : iterat) {
				System.out.println(tuple._1() + " AND " + tuple._2() + ": " + tuple._3());
			}
		}

		System.out.println("DONE!");

	}
}
