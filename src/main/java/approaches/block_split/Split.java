package approaches.block_split;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.Map.Entry;
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
import scala.Tuple2;
import scala.Tuple3;
import utils.OrderBy;
import utils.StringSimilarity;
public class Split {
    private static final Double LIMIAR = 0.7;
    
    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.err.println("Please provide the number of partitions, "
                    + "the number of workers, the input file full path as argument and the output path");
            System.exit(0);
        }
        
         // Number of partitions should be always bigger or equal than number of workers.
        Integer numPartitions = Integer.valueOf(args[0]);
        Integer numWorkers  =  Integer.valueOf(args[1]);
        String inputPath = args[2];
        String outputPath = args[3];
            
//        SparkConf conf = new SparkConf().setAppName("Block Split").setMaster("local");
      SparkConf conf = new SparkConf().setAppName("Block Split");
        JavaSparkContext context = new JavaSparkContext(conf);
        
        BDM bdm = new BDM();
        
        JavaRDD<String> inputEntities = context.textFile(inputPath, numPartitions);
        
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
        
        JavaPairRDD<String, String> additionalOutput = words.mapToPair(new PairFunction<String, String, String>() {
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
        
        bdm.setBDM(counterOcurrences);
        Broadcast<BDM> bdmBroadcast = context.broadcast(bdm);
        BDM bdmValue = bdmBroadcast.getValue();
        
        Map<Tuple3<Long, Long, Long>, Long> matchTasks = new HashMap<>();
        Long numBlocks = bdmValue.getNumBlocks();
        Long comps;
        Tuple3<Long, Long, Long> keyMatchTask;
        Long sizeBlockPartition;
        Long sizeBlockPartition2;
        Long numCompsPartition;
        for (Long k = 0L; k < numBlocks; k++) {
            comps = bdmValue.getNumberComparisonsPerBlock(k);
            boolean isAboveAverage = bdmValue.getNumberComparisonsPerBlock(k) > ((long) Math.ceil(bdmValue.getNumberComparisons() / (numWorkers *1.0)));
            if (comps > 0 && !isAboveAverage) {
                keyMatchTask = new Tuple3<Long, Long, Long>(k, 0L, 0L);
                matchTasks.put(keyMatchTask, comps);
                
            } else if (comps > 0) {
                for (Long i = 0L; i < numPartitions; i++) {
                    sizeBlockPartition = bdmValue.getSize(k, i);
                    for (Long j = 0L; j <= i; j++) {
                        sizeBlockPartition2 = bdmValue.getSize(k, j);
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
        
        Map<Long, Long> sum = new HashMap<>();
        matchTasks = OrderBy.orderByValueDescending(matchTasks);
        Map<Long, Long> reduces = new HashMap<>();
        for (Long i = 0L; i < numWorkers; i++) {
            reduces.put(i, 0L);
        }
        for (Tuple3<Long, Long, Long> key_map : matchTasks.keySet()) {
            Long numCompsBlock = matchTasks.get(key_map);
            Long nextReduce = 0L;
            Long min = bdmValue.getNumberComparisons();
            
            for (Long key : reduces.keySet()) {
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
            Long soma = sum.get(reduceTask) + matchTasks.get(key_map);
            sum.put(reduceTask, soma);
            
            matchTasks.put(key_map, reduceTask);
            
        }
        
        for (Entry<Long, Long> entry : sum.entrySet()) {
            System.out.println(entry.getKey() + " - " + entry.getValue());
        }
        
        Broadcast<Map<Tuple3<Long, Long, Long>, Long>> matchTasksBroadcast =  context.broadcast(matchTasks);
        
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
                BDM bdmValue = bdmBroadcast.getValue();
                Map<Tuple3<Long, Long, Long>, Long> matchTasksValue = matchTasksBroadcast.getValue();
                output  = new ArrayList<>();
                key_partition = t._1.split("\\.");
                key = key_partition[0];
                partitionIndex = Long.valueOf(key_partition[1]);
                blockIndex = bdmValue.blockIndex(key);
                comps = bdmValue.getNumberComparisonsPerBlock(blockIndex);          
                boolean isAboveAverage = bdmValue.getNumberComparisonsPerBlock(blockIndex) > ((long) Math.ceil(bdmValue.getNumberComparisons() / (numWorkers *1.0)));
                if (!isAboveAverage) {
                    
                    if (comps > 0) {
                        reduceTask = matchTasksValue.get(new Tuple3<Long, Long, Long>(blockIndex, 0L, 0L));
                        output.add(new Tuple2<String, String>(reduceTask + "." + blockIndex + "." + String.valueOf(0) + "." + String.valueOf(0) + ".*" , t._2));
                    
                    }
                    
                } else {
                    
                    for (int i = 0; i < numPartitions; i++) {
                        min = Math.min(partitionIndex, i);
                        max = Math.max(partitionIndex, i);
                        reduceTask = matchTasksValue.get(new Tuple3<Long, Long, Long>(blockIndex, max, min));
                        
                        if (reduceTask != null) {
                            output.add(new Tuple2<String, String>(reduceTask + "." + blockIndex + "." + max + "." + min + "." + partitionIndex, t._2));
                        }
                    }
                }
                return output.iterator();
            }
        });
        
        JavaPairRDD<String, String> sortedPairs = loadBalancing.mapToPair(new PairFunction<Tuple2<String,String>, String, String>() {
            @Override
            public Tuple2<String, String> call(Tuple2<String, String> s) throws Exception {
                return new Tuple2<String, String>(s._1, s._2);
            }
        }).sortByKey();
        
        
        JavaPairRDD<String, Iterable<EMResult>> groupByKey = sortedPairs.mapToPair(new PairFunction<Tuple2<String,String>, String, EMResult>() {
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
                
                return new Tuple2<String, EMResult>(part1+"."+part2+part3+part4, emr);
            }
        }).groupByKey(new Partitioner() {
            
            @Override
            public int numPartitions() {
                return numPartitions;
            }
            
            @Override
            public int getPartition(Object key) {
                return Integer.valueOf(((String)key).split("\\.")[0]);
            }
        });
        
        JavaRDD<Tuple3<String, String, Double>> matchResult = groupByKey.flatMap(new FlatMapFunction<Tuple2<String,Iterable<EMResult>>, Tuple3<String, String, Double>>() {
            private List<Tuple3<String, String, Double>> outputList;
            private List<EMResult> emResultOrdered;
            private Double similarity;
            
            @Override
            public Iterator<Tuple3<String, String, Double>> call(Tuple2<String, Iterable<EMResult>> tuple) throws Exception {
                
                outputList = new ArrayList<Tuple3<String, String, Double>>();               
                
                emResultOrdered = new ArrayList<>();
                for (EMResult result : tuple._2) {
                    emResultOrdered.add(result);
                }   
                
                EMResult emr;
                for (int i = 0; i < emResultOrdered.size()-1; i++) {
                    emr = emResultOrdered.get(i);
                    
                    EMResult emrNext;
                    for (int j = i + 1; j < emResultOrdered.size(); j++) {
                        emrNext = emResultOrdered.get(j);
                        
                        if (emr.getKeyPart5().equals("*") || emr.getKeyPart3().equals(emr.getKeyPart4()) || !emr.getKeyPart5().equals(emrNext.getKeyPart5())) {
                            similarity = StringSimilarity.similarity(emr.getEntity(), emrNext.getEntity());
                              if (similarity >= LIMIAR) {
                                outputList.add(new Tuple3<String, String, Double>(emr.getEntity(), emrNext.getEntity(), similarity));
                              }
                        }
                    }
                }
                        
                return outputList.iterator();
            }
        });
        
        matchResult.saveAsTextFile(outputPath);
        
        context.stop();
        context.close();
        System.out.println("DONE");
    }
    
}