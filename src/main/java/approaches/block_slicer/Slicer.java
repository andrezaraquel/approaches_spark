package approaches.block_slicer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.hdfs.server.namenode.HostFileManager.EntrySet;
import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;

import BDMSource.BDM;
import approaches.block_split.EMResult;
import jersey.repackaged.com.google.common.collect.Lists;
import scala.Tuple2;
import scala.Tuple3;
import utils.OrderBy;
import utils.StringSimilarity;

public class Slicer {

	private static final Double LIMIAR = 0.7;

	public static void main(String[] args) {

		if (args.length < 4) {
			System.err.println(
					"Usage: <Number of Partitions> <Number of Workers> <Path Input File> <Path Output Directory>");
			System.exit(0);
		}

		// Number of partitions should be always bigger or equal than number of workers.
		Integer numPartitions = Integer.valueOf(args[0]);
		Integer numWorkers = Integer.valueOf(args[1]);
		String inputPath = args[2];
		String outputPath = args[3];

//		SparkConf conf = new SparkConf().setAppName("Block Slicer").setMaster("local");
		 SparkConf conf = new SparkConf().setAppName("Block Slicer");
		JavaSparkContext context = new JavaSparkContext(conf);

		JavaRDD<String> inputEntities = context.textFile(inputPath, numPartitions);

		JavaRDD<String> words = inputEntities.flatMap(new FlatMapFunction<String, String>() {
			@Override
			public Iterator<String> call(String t) throws Exception {
				return Arrays.asList(t.split("\n")).iterator();
			}
		});

		JavaPairRDD<String, Long> blockingOrdered = words.mapToPair(new PairFunction<String, String, Long>() {
			@Override
			public Tuple2<String, Long> call(String s) throws Exception {
				return new Tuple2<String, Long>(BDM.getBlockingKey(s) + "." + TaskContext.getPartitionId(), 1L);
			}
		}).sortByKey();

		JavaPairRDD<String, Long> counterOcurrences = blockingOrdered.reduceByKey(new Function2<Long, Long, Long>() {
			@Override
			public Long call(Long a, Long b) throws Exception {
				return a + b;
			}
		});

		BDM bdm = new BDM();
		bdm.setBDM(counterOcurrences);
		Broadcast<BDM> bdmBroadcast = context.broadcast(bdm);

		Map<Tuple2<Long, Long>, Tuple2<Long, Long>> matchTasks = new HashMap<>();
		Long numBlocks = bdmBroadcast.getValue().getNumBlocks();
		Long comps;
		Tuple2<Long, Long> keyMatchTask;
		Tuple2<Long, Long> valueMatchTask;
		Long numEntities;

		for (Long k = 0L; k < numBlocks; k++) {
			comps = bdmBroadcast.getValue().getNumberComparisonsPerBlock(k);
			numEntities = bdmBroadcast.getValue().getBlockSize(k);
			long getCompsPerReduceTask = (long) Math.ceil(bdmBroadcast.getValue().getNumberComparisons() / (numWorkers * 1.0));
			boolean isAboveAverage = bdmBroadcast.getValue().getNumberComparisonsPerBlock(k) > getCompsPerReduceTask;
			if (comps > 0 && !isAboveAverage) {
				keyMatchTask = new Tuple2<Long, Long>(k, 0L);
				valueMatchTask = new Tuple2<Long, Long>(comps, numEntities);
				matchTasks.put(keyMatchTask, valueMatchTask);

			} else if (comps > 0) {
				boolean enableSlice = true;
				Long indexSlice = 0L;
				while (enableSlice) {
					indexSlice++;
					Long numOfNonReplicatedEntities = numOfNonReplicatedEntities(numEntities, getCompsPerReduceTask);
					Long numberComparisonsSliced = 0L;

					if (numOfNonReplicatedEntities == 0) {
						numberComparisonsSliced = (numEntities * (numEntities - 1)) / 2;
					} else {
						numberComparisonsSliced = ((numOfNonReplicatedEntities * (numOfNonReplicatedEntities - 1)) / 2)
								+ ((numEntities - numOfNonReplicatedEntities) * numOfNonReplicatedEntities);
					}

					if (numOfNonReplicatedEntities == 0) {
						enableSlice = false;
						keyMatchTask = new Tuple2<Long, Long>(k, 0L);
						valueMatchTask = new Tuple2<Long, Long>(numberComparisonsSliced, numEntities);

					} else {
						keyMatchTask = new Tuple2<Long, Long>(k, indexSlice);
						valueMatchTask = new Tuple2<Long, Long>(numberComparisonsSliced, numOfNonReplicatedEntities);
					}

					matchTasks.put(keyMatchTask, valueMatchTask);
					numEntities -= numOfNonReplicatedEntities;
				}

			}
		}
		Map<Long, Long> sum = new HashMap<>();
		matchTasks = OrderBy.orderByComparisonsDescending(matchTasks);
		Map<Long, Long> reduces = new HashMap<>();
		for (Long i = 0L; i < numWorkers; i++) {
			reduces.put(i, 0L);
		}

		for (Tuple2<Long, Long> key_map : matchTasks.keySet()) {
			Long numCompsBlock = matchTasks.get(key_map)._1;

			Long nextReduce = 0L;
			Long min = bdmBroadcast.getValue().getNumberComparisons();
			Set<Long> keySetReduces = reduces.keySet();
			for (Long key : keySetReduces) {
				if (reduces.get(key) < min) {
					min = reduces.get(key);
					nextReduce = key;
				}
			}

			reduces.put(nextReduce, min + numCompsBlock);

			Long reduceTask = nextReduce;
			if (!sum.containsKey(reduceTask)) {
				sum.put(reduceTask, 0L);
			}
			Long soma = sum.get(reduceTask) + matchTasks.get(key_map)._1;
			sum.put(reduceTask, soma);
			// System.out.println(reduceTask + ";" +
			// matchTasks.get(key_map)._1);
			matchTasks.put(key_map, new Tuple2<Long, Long>(reduceTask, matchTasks.get(key_map)._2));

		}

		for (Entry<Long, Long> entry : sum.entrySet()) {
			System.out.println(entry.getKey() + " - " + entry.getValue());
		}

		Broadcast<Map<Tuple2<Long, Long>, Tuple2<Long, Long>>> matchTasksBroadcast = context.broadcast(matchTasks);

		JavaPairRDD<String, Iterable<String>> additionalOutput = words.mapToPair(new PairFunction<String, String, String>() {
			@Override
			public Tuple2<String, String> call(String t) throws Exception {
				String value = t;
				if (value.isEmpty()) {
					value = "<><><>";
				}
				return new Tuple2<String, String>(BDM.getBlockingKey(t), value);
			}
		}).groupByKey(numPartitions);

		JavaRDD<Tuple2<String, String>> map = additionalOutput
				.flatMap(new FlatMapFunction<Tuple2<String, Iterable<String>>, Tuple2<String, String>>() {

					private Long reduceTask;
					private Long blockIndex;
					private String key;
					private Long comps;
					private List<Tuple2<String, String>> output;

					@Override
					public Iterator<Tuple2<String, String>> call(Tuple2<String, Iterable<String>> t) throws Exception {
						Map<Tuple2<Long, Long>, Tuple2<Long, Long>> matchTasksValue = matchTasksBroadcast.getValue();
						key = t._1;
						blockIndex = bdmBroadcast.getValue().blockIndex(key);
						output = new ArrayList<>();
						comps = bdmBroadcast.getValue().getNumberComparisonsPerBlock(blockIndex);
						long getCompsPerReduceTask = (long) Math
								.ceil(bdmBroadcast.getValue().getNumberComparisons() / (numWorkers * 1.0));
						boolean isAboveAverage = bdmBroadcast.getValue()
								.getNumberComparisonsPerBlock(blockIndex) > getCompsPerReduceTask;
						if (!isAboveAverage) {

							for (String value : t._2) {
								if (comps > 0) {
									reduceTask = matchTasksValue.get(new Tuple2<Long, Long>(blockIndex, 0L))._1;
									output.add(new Tuple2<String, String>(reduceTask + "." + blockIndex, value));
								}
							}
						} else {

							List<String> values = new ArrayList<>();
							for (String value : t._2) {
								values.add(value);
							}

							Tuple2<Long, Long> valueTask;
							Long numOfNonReplicatedEntities;
							Long slicePoint = 0L;
							String keyOutput;

							boolean enableSlice = true;
							Long indexSlice = 0L;
							while (enableSlice) {

								valueTask = matchTasksValue.get(new Tuple2<Long, Long>(blockIndex, indexSlice));
								if (valueTask == null) {
									enableSlice = false;

								} else {
									reduceTask = valueTask._1;

									numOfNonReplicatedEntities = valueTask._2;
									keyOutput = reduceTask + "." + blockIndex + "." + indexSlice;

									if (indexSlice > 1) {

										for (Long i = slicePoint; i < values.size(); i++) {
											if (i >= numOfNonReplicatedEntities + slicePoint) {
												output.add(new Tuple2<String, String>(keyOutput,
														values.get(i.intValue()) + ".<*>"));
											} else {
												output.add(new Tuple2<String, String>(keyOutput,
														values.get(i.intValue())));
											}
										}

										slicePoint += numOfNonReplicatedEntities;

									} else if (indexSlice == 1) {

										slicePoint += numOfNonReplicatedEntities;

										for (Long i = 0L; i < values.size(); i++) {
											if (i >= numOfNonReplicatedEntities) {
												output.add(new Tuple2<String, String>(keyOutput,
														values.get(i.intValue()) + ".<*>"));
											} else {
												output.add(new Tuple2<String, String>(keyOutput,
														values.get(i.intValue())));
											}
										}

									} else {
										for (Long i = values.size() - numOfNonReplicatedEntities; i < values
												.size(); i++) {
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

		JavaPairRDD<String, Iterable<String>> groupedMap = map
				.mapToPair(new PairFunction<Tuple2<String, String>, String, String>() {
					@Override
					public Tuple2<String, String> call(Tuple2<String, String> t) throws Exception {
						return new Tuple2<String, String>(t._1, t._2);

					}

				}).groupByKey(new Partitioner() {

					@Override
					public int numPartitions() {
						return numPartitions;
					}

					@Override
					public int getPartition(Object key) {
						return Integer.valueOf(((String) key).split("\\.")[0]);
					}
				});


		JavaRDD<Tuple3<String, String, Double>> matchResult = groupedMap
				.flatMap(new FlatMapFunction<Tuple2<String, Iterable<String>>, Tuple3<String, String, Double>>() {

					@Override
					public Iterator<Tuple3<String, String, Double>> call(Tuple2<String, Iterable<String>> t)
							throws Exception {

						List<Tuple3<String, String, Double>> list = new ArrayList<Tuple3<String, String, Double>>();
						Iterable<String> values = t._2;

						String currentEntity;
						String nextEntity;
						Double similarity;

						List<String> nomMarked = new ArrayList<>();
						List<String> marked = new ArrayList<>();

						for (Iterator<String> iterator = values.iterator(); iterator.hasNext();) {
							currentEntity = iterator.next();
							if (currentEntity.contains(".<*>")) {
								currentEntity = currentEntity.substring(0, currentEntity.lastIndexOf(".<*>"));
								marked.add(currentEntity);
							} else {
								nomMarked.add(currentEntity);
							}							
						}
						
						for (Integer i = 0; i < nomMarked.size(); i++) {
							currentEntity = nomMarked.get(i);
							for (Integer j = i+1; j < nomMarked.size(); j++) {
								nextEntity = nomMarked.get(j);
								similarity = StringSimilarity.similarity(currentEntity, nextEntity);
								 if (similarity >= LIMIAR) {
									 list.add(new Tuple3<String, String, Double>(currentEntity, nextEntity,
										similarity));

								 }
							}
							
							for (Integer k = 0; k < marked.size(); k++) {
								nextEntity = marked.get(k);
								similarity = StringSimilarity.similarity(currentEntity, nextEntity);
								 if (similarity >= LIMIAR) {
									 list.add(new Tuple3<String, String, Double>(currentEntity, nextEntity,
										similarity));

								 }
							}
						}

						return list.iterator();
					}
				});

		 matchResult.saveAsTextFile(outputPath);

		context.stop();
		context.close();
		System.out.println("DONE");
	}

	private static Long numOfNonReplicatedEntities(Long k_size, Long compsPerReduceTask) {
		Long b = 2 * k_size - 1;
		Long c = 2 * compsPerReduceTask;
		Double delta = Math.pow(b, 2) - (4 * c);
		return (long) ((b - Math.sqrt(delta)) / 2);
	}
}