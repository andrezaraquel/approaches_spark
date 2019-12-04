package approaches.block_slicer;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

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
	private static Map<Tuple2<Long, Long>, Tuple2<Long, Long>> matchTasks;
	private JavaPairRDD<String, String> additionalOutput;
	private static JavaRDD<Tuple3<String, String, Double>> reduce;
	
	public Job2(Job1 job1, Map<Tuple2<Long, Long>, Tuple2<Long, Long>> matchTasks) {
		this.job1 = job1;
		this.matchTasks = matchTasks;
		additionalOutput = job1.getAdditionalOutput();
		reduce = computationJob2(additionalOutput);
	}

	private static JavaRDD<Tuple3<String, String, Double>> computationJob2(JavaPairRDD<String, String> additionalOutput) {
		
		JavaPairRDD<String, Iterable<String>> groupByKey = additionalOutput.mapToPair(new PairFunction<Tuple2<String,String>, String, String>() {

			@Override
			public Tuple2<String, String> call(Tuple2<String, String> t) throws Exception {
				
				String[] keyPartition = t._1.split("\\.");
				String key = keyPartition[0];				
				return new Tuple2<String, String>(key, t._2);
			}
			
		}).groupByKey();
		
		JavaRDD<Tuple2<String, String>> map = groupByKey.flatMap(new FlatMapFunction<Tuple2<String, Iterable<String>>, Tuple2<String, String>>() {
			
			private Long reduceTask;
			private Long blockIndex;
			private String key;
			private Long comps;
			private List<Tuple2<String, String>> output;

			@Override
			public Iterator<Tuple2<String, String>> call(Tuple2<String, Iterable<String>> t) throws Exception {
				key = t._1;
				blockIndex = job1.getBdmBroadcast().blockIndex(key);
				output = new ArrayList<>();
				comps = job1.getBdmBroadcast().getNumberComparisonsPerBlock(blockIndex);

				if (!job1.isAboveAverage(blockIndex)) {
					
					for (String value : t._2) {
						if (comps > 0) {
							reduceTask = matchTasks
									.get(new Tuple2<Long, Long>(blockIndex, 0L))._1;
							output.add(new Tuple2<String, String>(reduceTask + "." + blockIndex, value));

						} else {
							output.add(new Tuple2<String, String>(value, value));
						}
					}
				} else {
					
					List<String> values = new ArrayList<>();
					for (String value : t._2) {
						values.add(value);
					}
					
					Tuple2<Long, Long> valueTask;
					Long numOfNonReplicatedEntities;
					String keyOutput;
					Long slicePoint;
					
					boolean enableSlice = true;
					Long indexSlice = 0L;
					while(enableSlice) {
						
						valueTask = matchTasks.get(new Tuple2<Long, Long>(blockIndex, indexSlice)); 
						if (valueTask == null) {
							enableSlice = false;
							
						} else {
							reduceTask = valueTask._1;
							numOfNonReplicatedEntities  = valueTask._2;
							keyOutput = reduceTask + "." + blockIndex + "." + indexSlice;
							
							if (indexSlice > 0) {
								slicePoint = indexSlice-1;
								
								for (Long i = slicePoint; i < values.size(); i++) {
									if (i >= numOfNonReplicatedEntities + slicePoint) {
										output.add(new Tuple2<String, String>(keyOutput, values.get(i.intValue()) + ".<*>"));
									} else {
										output.add(new Tuple2<String, String>(keyOutput, values.get(i.intValue())));
									}
								}
								
							} else {
								for (Long i = values.size() - numOfNonReplicatedEntities; i < values.size(); i++) {
									output.add(new Tuple2<String, String>(keyOutput, values.get(i.intValue())));
								}								
							}
						}
						indexSlice++;
					}
				}

				return output.iterator();
			}
		});
				
		JavaPairRDD<String, Iterable<String>> groupedMap = map.mapToPair(new PairFunction<Tuple2<String,String>, String, String>() {

			@Override
			public Tuple2<String, String> call(Tuple2<String, String> t) throws Exception {
				return t;
			}
			
		}).groupByKey();
		
		JavaRDD<Tuple3<String, String, Double>> matchResult = groupedMap.flatMap(new FlatMapFunction<Tuple2<String,Iterable<String>>, Tuple3<String, String, Double>>() {

			@Override
			public Iterator<Tuple3<String, String, Double>> call(Tuple2<String, Iterable<String>> t) throws Exception {
				
				List<Tuple3<String, String, Double>> list = new ArrayList<Tuple3<String, String, Double>>();	
				Iterable<String> values =  t._2;
				String word;
				String wordNext;
				Double similarity;
				
				int count = 0;
				int it;
				
				for (Iterator iterator = values.iterator(); iterator.hasNext();) {
					
					word = (String) iterator.next();
					if (!word.contains(".<*>")) {
						count++;	
						it = 0;
						for (Iterator iteratorNext = values.iterator(); iteratorNext.hasNext();) {
							
							for (int i = it; i < count; i++) {
								it++;
								iteratorNext.next();
							}
							
							if (iteratorNext.hasNext()) {
								wordNext = (String) iteratorNext.next();
								if (wordNext.contains(".<*>")) {
									wordNext = wordNext.substring(0, wordNext.lastIndexOf(".<*>"));
								}
								
								similarity = StringSimilarity.similarity(word, wordNext);
								if (similarity >= LIMIAR) {
									list.add(new Tuple3<String, String, Double>(word, wordNext, similarity));
								}
							}
						}
					}
				}				
				return list.iterator();
			}
		});
							
		return matchResult; 
	}
	
	public JavaRDD<Tuple3<String, String, Double>> getResult() {
		return this.reduce;
	}
}
