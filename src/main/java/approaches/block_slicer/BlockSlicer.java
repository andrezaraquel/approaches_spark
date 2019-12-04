package approaches.block_slicer;

import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import BDMSource.Job1;
import scala.Tuple2;
import scala.Tuple3;
import utils.OrderBy;

public class BlockSlicer {
	
	 //<blockIndex, indexSlice>, <numberComparisons, numOfNonReplicatedEntities>
	private static Map<Tuple2<Long, Long>, Tuple2<Long, Long>> matchTasks;
	private static Job1 job1;
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
		
//		SparkConf conf = new SparkConf().setAppName("Block Slicer").setMaster("local");
		SparkConf conf = new SparkConf().setAppName("Block Slicer");
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
		Long numBlocks = job1.getBdmBroadcast().getNumBlocks();
		Long comps;
		Tuple2<Long, Long> keyMatchTask;
		Tuple2<Long, Long> valueMatchTask;
		Long numEntities;
		
		for (Long k = 0L; k < numBlocks; k++) {
			comps = job1.getBdmBroadcast().getNumberComparisonsPerBlock(k);
			numEntities = job1.getBdmBroadcast().getBlockSize(k);
			
			if (comps > 0 && !job1.isAboveAverage(k)) {
				keyMatchTask = new Tuple2<Long, Long>(k, 0L);
				valueMatchTask = new Tuple2<Long, Long> (comps, numEntities);
				matchTasks.put(keyMatchTask, valueMatchTask);
				
			} else if (comps > 0) {
				
				boolean enableSlice = true;
				Long indexSlice = 0L;
				while(enableSlice) {
					indexSlice++;
					Long numOfNonReplicatedEntities = numOfNonReplicatedEntities(numEntities, job1.getCompsPerReduceTask()); 
					
					Long numberComparisonsSliced = 0L;
					for (Long i = numOfNonReplicatedEntities+1; i < numEntities; i++) {
						numberComparisonsSliced += i;
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
		
		matchTasks = OrderBy.orderByComparisonsDescending(matchTasks);
		reduces = new HashMap<>();
		for (Long i = 0L; i < job1.getNumWorkers(); i++) {
			reduces.put(i, 0L);
		}

		for (Tuple2<Long, Long> key_map : matchTasks.keySet()) {
			Long numCompsBlock = matchTasks.get(key_map)._1;
			Long reduceTask = getNextReduceTask(numCompsBlock);
			matchTasks.put(key_map, new Tuple2<Long, Long>(reduceTask, matchTasks.get(key_map)._2));
			System.out.println(key_map + "= " + matchTasks.get(key_map));
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

	public static Long numOfNonReplicatedEntities(Long k_size, Long compsPerReduceTask){
		Long b = 2 * k_size - 1;
		Long c = 2 * compsPerReduceTask;
		Double delta = Math.pow(b, 2) - (4 * c);
		return (long) ((b - Math.sqrt(delta))/2);
	}
}
