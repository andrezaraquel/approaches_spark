package approaches.block_split;

import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import BDMSource.Job1;
import scala.Tuple3;
import utils.OrderBy;

public class BlockSplit {

	private static Job1 job1;
	private static Long numBlocks;
	private static Map<Tuple3<Long, Long, Long>, Long> matchTasks;
	private static Map<Long, Long> reduces;

	public static void main(String[] args) throws FileNotFoundException {

		if (args.length < 4) {
			System.err.println("Please provide the number of partitions, "
					+ "the number of workers, the input file full path as argument and the output path");
			System.exit(0);
		}
		
		 // Number of partitions should be always bigger or equal than number of workers.
		Long numPartitions = Long.parseLong(args[0]);
		Long numWorkers  = Long.parseLong(args[1]);
		String outputPath = args[3];
			
//		SparkConf conf = new SparkConf().setAppName("Block Split").setMaster("local");
		SparkConf conf = new SparkConf().setAppName("Block Split");
		JavaSparkContext context = new JavaSparkContext(conf);
		job1 = new Job1(context, args[2], numPartitions, numWorkers);		
		
		map_configure();

		Job2 job2 = new Job2(job1, matchTasks);
		
		for (Tuple3<String, String, Double> tuple : job2.getResult().collect()){
			System.out.println(tuple._1() + " - " + tuple._2() + " = " + tuple._3());
		};
		
//		job2.getResult().saveAsTextFile(outputPath);
		
		context.stop();
		context.close();
		System.out.println("DONE");
	}
	
	private static void map_configure() {
		matchTasks = new HashMap<>();
		numBlocks = job1.getBdmBroadcast().getNumBlocks();
		Long comps;
		Tuple3<Long, Long, Long> keyMatchTask;
		Long sizeBlockPartition;
		Long sizeBlockPartition2;
		Long numCompsPartition;

		for (Long k = 0L; k < numBlocks; k++) {
			comps = job1.getBdmBroadcast().getNumberComparisonsPerBlock(k);
			if (comps > 0 && !job1.isAboveAverage(k)) {
				keyMatchTask = new Tuple3<Long, Long, Long>(k, 0L, 0L);
				matchTasks.put(keyMatchTask, comps);
				
			} else if (comps > 0) {
				for (Long i = 0L; i < job1.getNumPartitions(); i++) {
					sizeBlockPartition = job1.getBdmBroadcast().getSize(k, i);

					for (Long j = 0L; j <= i; j++) {
						sizeBlockPartition2 = job1.getBdmBroadcast().getSize(k, j);

						if ((sizeBlockPartition * sizeBlockPartition2) > 0) {
							if (i == j) {
								keyMatchTask = new Tuple3<Long, Long, Long>(k, i, j);
								numCompsPartition = (long) Math.ceil(0.5 * sizeBlockPartition * (sizeBlockPartition - 1));
								matchTasks.put(keyMatchTask, numCompsPartition);

							} else {
								keyMatchTask = new Tuple3<Long, Long, Long>(k, i, j);
								numCompsPartition = (long) Math.ceil(sizeBlockPartition * sizeBlockPartition2);
								matchTasks.put(keyMatchTask, numCompsPartition);
							}
						}
					}
				}
			}
		}
		
		matchTasks = OrderBy.orderByValueDescending(matchTasks);
		reduces = new HashMap<>();
		for (Long i = 0L; i < job1.getNumWorkers(); i++) {
			reduces.put(i, 0L);
		}

		for (Tuple3<Long, Long, Long> key_map : matchTasks.keySet()) {
			Long numCompsBlock = matchTasks.get(key_map);
			Long reduceTask = getNextReduceTask(numCompsBlock);
			matchTasks.put(key_map, reduceTask);
		}
	}

	public static Long getNextReduceTask(Long numCompsBlock) {
		Long nextReduce = 0L;
		Long min = job1.getBdmBroadcast().getNumberComparisons();
		
		for (Long key : reduces.keySet()) {
			if (reduces.get(key) < min) {
				min = reduces.get(key);
				nextReduce = key;
			}
		}
		
		reduces.put(nextReduce, min + numCompsBlock);
		return nextReduce;
	}
}
