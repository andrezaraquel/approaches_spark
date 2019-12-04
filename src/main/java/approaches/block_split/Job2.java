package approaches.block_split;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import BDMSource.Job1;
import scala.Tuple2;
import scala.Tuple3;
import utils.StringSimilarity;

public class Job2{

	private static final Double LIMIAR = 0.7;
	
	private static Job1 job1;
	private static Map<Tuple3<Long, Long, Long>, Long> matchTasks;
	private JavaPairRDD<String, String> additionalOutput;
	private static JavaRDD<Tuple3<String, String, Double>> reduce;
	
	public Job2(Job1 job1, Map<Tuple3<Long, Long, Long>, Long> matchTasks) {
		this.job1 = job1;
		this.matchTasks = matchTasks;
		additionalOutput = job1.getAdditionalOutput();
		reduce = computationJob2(additionalOutput);
		//reduce.saveAsTextFile("reduce");
	}

	private static JavaRDD<Tuple3<String, String, Double>> computationJob2(JavaPairRDD<String, String> additionalOutput) {
		JavaRDD<Tuple2<String, String>> loadBalancing = additionalOutput.flatMap(new FlatMapFunction<Tuple2<String,String>, Tuple2<String, String>>() {

			private String[] key_partition;
			private String key;
			private Long partitionIndex;
			private Long blockIndex;
			private Long comps;
			private Long reduceTask;
			private Long min;
			private Long max;
			private List<Tuple2<String, String>> output;
			
			@Override
			public Iterator<Tuple2<String, String>> call(Tuple2<String, String> t) throws Exception {
				output  = new ArrayList<>();
				key_partition = t._1.split("\\.");
				key = key_partition[0];
				partitionIndex = Long.valueOf(key_partition[1]);
				blockIndex = job1.getBdmBroadcast().blockIndex(key);
				comps = job1.getBdmBroadcast().getNumberComparisonsPerBlock(blockIndex);			
				
				if (!job1.isAboveAverage(blockIndex)) {
					
					if (comps > 0) {
						reduceTask = matchTasks.get(new Tuple3<Long, Long, Long>(blockIndex, 0L, 0L));
						output.add(new Tuple2<String, String>(reduceTask + "." + blockIndex + "." + String.valueOf(0) + "." + String.valueOf(0) + ".*" , t._2));
					
					}
					
				} else {
					
					for (int i = 0; i < job1.getNumPartitions(); i++) {
						min = Math.min(partitionIndex, i);
						max = Math.max(partitionIndex, i);
						reduceTask = matchTasks.get(new Tuple3<Long, Long, Long>(blockIndex, max, min));
						
						if (reduceTask != null) {
							output.add(new Tuple2<String, String>(reduceTask + "." + blockIndex + "." + max + "." + min + "." + partitionIndex, t._2));
						}
					}
				}
				return output.iterator();
			}
		});
		
		JavaPairRDD<String, EMResult> loadBalancingPairs = loadBalancing.mapToPair(new PairFunction<Tuple2<String,String>, String, EMResult>() {

			@Override
			public Tuple2<String, EMResult> call(Tuple2<String, String> s) throws Exception {
				
				String key = s._1;
				String value = s._2;
				StringTokenizer keyParts = new StringTokenizer(key, ".");
				
				String part1 = keyParts.nextToken();
				String part2 = keyParts.nextToken();
				String part3 = keyParts.nextToken();
				String part4 = keyParts.nextToken();
				String part5 = keyParts.nextToken();
				

				EMResult emr = new EMResult();
				emr.setKeyPart1(part1);
				emr.setKeyPart2(part2);
				emr.setKeyPart3(part3);
				emr.setKeyPart4(part4);
				emr.setKeyPart5(part5);
				emr.setEntity(value);
				
				return new Tuple2<String, EMResult>(part1, emr);
			}
		}).sortByKey();
		
		JavaPairRDD<String, Iterable<EMResult>> groupByKey = loadBalancingPairs.groupByKey();

		JavaRDD<Tuple3<String, String, Double>> matchResult = groupByKey.flatMap(new FlatMapFunction<Tuple2<String,Iterable<EMResult>>, Tuple3<String, String, Double>>() {

			private List<Tuple3<String, String, Double>> outputList;
			private Iterable<EMResult> emResultList;
			private List<EMResult> emResultOrdered;
			private Double similarity;
			
			@Override
			public Iterator<Tuple3<String, String, Double>> call(Tuple2<String, Iterable<EMResult>> tuple) throws Exception {
				
				outputList = new ArrayList<Tuple3<String, String, Double>>();				
				
				emResultList = tuple._2;
				emResultOrdered = new ArrayList<>();
				for (EMResult result : emResultList) {
					emResultOrdered.add(result);
				}				
				Collections.sort(emResultOrdered);
				
				EMResult emr;
				String key;
				for (int i = 0; i < emResultOrdered.size()-1; i++) {
					emr = emResultOrdered.get(i);
					key = emr.getKeyPart2() + emr.getKeyPart3() + emr.getKeyPart4();
					
					EMResult emrNext;
					String keyNext;
					for (int j = i + 1; j < emResultOrdered.size(); j++) {
						emrNext = emResultOrdered.get(j);
						keyNext = emrNext.getKeyPart2() + emrNext.getKeyPart3() + emrNext.getKeyPart4();
						
						if (key.equals(keyNext)) {
							if (emr.getKeyPart5().equals("*") || emr.getKeyPart3().equals(emr.getKeyPart4()) || !emr.getKeyPart5().equals(emrNext.getKeyPart5())) {
								similarity = StringSimilarity.similarity(emr.getEntity(), emrNext.getEntity());
								if (similarity >= LIMIAR) {
									outputList.add(new Tuple3<String, String, Double>(emr.getAllKey() + emr.getEntity(), emrNext.getAllKey() + emrNext.getEntity(), similarity));
								}
							}
						} else {
							break;
						}
					}
				}
						
				return outputList.iterator();
			}
		});
			
		return matchResult; 
	}

	public JavaRDD<Tuple3<String, String, Double>> getResult() {
		return this.reduce;
	}
}
