package BDMSource;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;

import scala.Tuple2;

public class BDM implements Serializable {

	private static final long serialVersionUID = 1L;
	private List<BDMData> bdm;
	private HashMap<String, Long> blockingkeyIndexer;
	private Long numberComparisonsBDM;

	public BDM() {
		this.bdm = new ArrayList<BDMData>();		
		blockingkeyIndexer = new HashMap<String, Long>();
		numberComparisonsBDM = 0L;
	}
	
	public void setBDM(JavaPairRDD<String, Long> rdd){		
		String[] key_partition = null;
		String key = null;
		Long partition = null;
		Long size = null;
		Long  blockingPos = 0L;
		Long index;
		
		for(Tuple2<String, Long> tuple : rdd.collect()){
			key_partition = tuple._1.split("\\.");
			key = key_partition[0];
			partition = Long.valueOf(key_partition[1]);
			size = tuple._2;
			index = blockingkeyIndexer.get(key);
			
			if (bdm.isEmpty() ||  index == null) {
				BDMData bdmData = new BDMData(key, partition, size);
				bdm.add(bdmData);
				blockingkeyIndexer.put(key, blockingPos);
				
				blockingPos++;				
				
			} else {				
				bdm.get(index.intValue()).addPartition(partition, size);
			}
		}
		
		setNumberComparisonsBDM();
	}

	public boolean isEmpty() {
		return this.bdm.isEmpty();
	}

	public void addData(BDMData data) {
		this.bdm.add(data);
	}
	
	public int getSize() {
		return this.bdm.size();
	}

	public BDMData getData(Long index) {
		return this.bdm.get(index.intValue());
	}

	private void setNumberComparisonsBDM() {
		for (Long i = 0L; i < this.blockingkeyIndexer.size(); i++) {
			this.numberComparisonsBDM += getNumberComparisonsPerBlock(i);
		}
	}

	public Long getNumberComparisons() {
		return this.numberComparisonsBDM;
	}

	public Long getNumberComparisonsPerBlock(Long blockIndex) {
		Long blockSize = getBlockSize(blockIndex);
		return (long) Math.ceil((0.5) * blockSize * (blockSize - 1));
	}

	public Long getBlockSize(Long blockIndex) {
		return bdm.get(blockIndex.intValue()).getSizeBlock();
	}

	public static String getBlockingKey(String entity) {
		if (entity.length() < 3){
			return ">>>";
		}
		return entity.substring(0, 3).replace(".", ",").toUpperCase();
	}
	
	public HashMap<String, Long> getBlockingkeyIndexer() {
		return blockingkeyIndexer;
	}

	public void setBlockingkeyIndexer(HashMap<String, Long> blockingkeyIndexer) {
		this.blockingkeyIndexer = blockingkeyIndexer;
	}
			
	public Long blockIndex(String blockingkey) {		
		 return blockingkeyIndexer.get(blockingkey);
	}	
		
	public Long getSize(Long block, Long partition) {
		return bdm.get(block.intValue()).getSizePartition(partition);
	}
	
	public Long getNumBlocks() {
		return (long) bdm.size();
	}		
}
