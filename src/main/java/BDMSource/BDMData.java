package BDMSource;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class BDMData implements Serializable {
	
	private static final long serialVersionUID = 1L;
	private String key;
	private Map<Long, Long> partitions;
	
	public BDMData(String key, Long partition, Long size) {
		this.key = key;
		this.partitions = new HashMap<Long, Long>();
		this.partitions.put(partition, size);
	}

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public Map<Long, Long> getPartitions() {
		return partitions;
	}

	public void setPartitions(Map<Long, Long> partition) {
		this.partitions = partition;
	}
	
	public void addPartition(Long partition, Long size) {
		this.partitions.put(partition, size);
	}

	public Long getSizePartition(Long partition) {
		
		if (!partitions.containsKey(partition)) {
			return 0L;
		}
		
		return partitions.get(partition);
	}
	
	public Long getSizeBlock() {
		
		Long blockSize = 0L;
		for (Long value : partitions.values()) {
			blockSize += value;
		}
		
		return blockSize;
	}

	@Override
	public String toString() {
		return "BDMData [key=" + key + ", partitions=" + partitions + "]";
	}
}
