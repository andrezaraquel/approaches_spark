package BDMSource;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;

import scala.Tuple2;

public class Job1 {

	private static BDM bdm;
	private JavaSparkContext sc;
	private String file_path;
	private Long num_partitions;
	private Long num_workers;		
	private static JavaPairRDD<String, String> additionalOutput;
	private Broadcast<BDM> bdmBroadcast;
	
	public Job1(JavaSparkContext context, String file_path, Long num_partitions, Long num_workers){
		
		bdm = new BDM();		
		this.sc = context;
		this.file_path = file_path;
		this.num_partitions = num_partitions;
		this.num_workers = num_workers;	
		
		JavaPairRDD<String, Long> counterOcurrences = computationJob1(sc, this.file_path, this.num_partitions);	
		bdm.setBDM(counterOcurrences);
		bdmBroadcast = sc.broadcast(bdm);
		
		//counterOcurrences.saveAsTextFile("outputCO");	
	}
	
	public static JavaPairRDD<String, Long> computationJob1(JavaSparkContext context, String path, Long num_partitions){
		
		JavaRDD<String> inputEntities = context.textFile(path, num_partitions.intValue());
		
		JavaRDD<String> words = inputEntities.flatMap(new FlatMapFunction<String, String>(){
			@Override
			public Iterator<String> call(String t) throws Exception {
				return Arrays.asList(t.split("\n")).iterator();
			}			
		});
		
		JavaPairRDD<String, Long> blockingOrdered = words.mapToPair(new PairFunction<String, String, Long>() {
			@Override
			public Tuple2<String, Long> call(String s) throws Exception {
				return new Tuple2<String, Long>(bdm.getBlockingKey(s) + "." + TaskContext.getPartitionId(), 1L);
			}
		}).sortByKey();
		
		additionalOutput = words.mapToPair(new PairFunction<String, String, String>() {
			@Override
			public Tuple2<String, String> call(String t) throws Exception {
				return new Tuple2<String, String>(bdm.getBlockingKey(t)+ "." + TaskContext.getPartitionId(), t);
			}
		});
		
		JavaPairRDD<String, Long> counterOcurrences = blockingOrdered.reduceByKey(new Function2<Long, Long, Long>() {
			@Override
			public Long call(Long a, Long b) throws Exception {
				return a + b;
			}
		});
		
		return counterOcurrences;
	}	
	
	public BDM getBdmBroadcast() {
		return bdmBroadcast.getValue();
	}

	public Long getNumPartitions() {
		return num_partitions;
	}
	
	public Long getNumWorkers() {
		return num_workers;
	}
	
	public JavaPairRDD<String, String> getAdditionalOutput() {
		return additionalOutput;
	}

	public long getCompsPerReduceTask(){
		return (long) Math.ceil(bdm.getNumberComparisons() / (getNumWorkers()*1.0));
	}

	public boolean isAboveAverage(Long blockIndex) {
		return bdm.getNumberComparisonsPerBlock(blockIndex) > getCompsPerReduceTask();
	}
}