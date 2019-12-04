package approaches.block_split;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class EMResult implements Serializable, Comparable<EMResult>{
	
	private static final long serialVersionUID = 1L;
	private String keyPart1;
	private String keyPart2;
	private String keyPart3;
	private String keyPart4;
	private String keyPart5;
	private String entity;
	private List<String> result;
	
	public EMResult() {		
		result =  new ArrayList<>(); // primeira palavra - segunda palavra = similaridade
	}
	
	public String getAllKey(){
		return this.keyPart1+this.keyPart2+this.keyPart3+this.keyPart4+this.keyPart5;
	}
	
	@Override
	public String toString() {
		return "EMResult [key=" +keyPart1+ "."+ keyPart2 + "." + keyPart3 + "." + keyPart4 + "]";
	}

	public String getKeyPart1() {
		return keyPart1;
	}

	public void setKeyPart1(String keyPart1) {
		this.keyPart1 = keyPart1;
	}
	
	public String getKeyPart2() {
		return keyPart2;
	}

	public void setKeyPart2(String keyPart2) {
		this.keyPart2 = keyPart2;
	}

	public String getKeyPart3() {
		return keyPart3;
	}

	public void setKeyPart3(String keyPart3) {
		this.keyPart3 = keyPart3;
	}

	public String getKeyPart4() {
		return keyPart4;
	}

	public void setKeyPart4(String keyPart4) {
		this.keyPart4 = keyPart4;
	}

	public String getKeyPart5() {
		return keyPart5;
	}

	public void setKeyPart5(String keyPart5) {
		this.keyPart5 = keyPart5;
	}

	public List<String> getResult() {
		return result;
	}

	public void setResult(List<String> result) {
		this.result = result;
	}
	
	public void addResult (String value) {
		this.result.add(value);
	}

	public String getEntity() {
		return entity;
	}

	public void setEntity(String entity) {
		this.entity = entity;
	}
	
	@Override
	public int compareTo(EMResult o) {
		Integer localKey = Integer.valueOf(this.keyPart2+this.keyPart3+this.keyPart4);
		Integer paramKey = Integer.valueOf(o.getKeyPart2()+o.getKeyPart3()+o.getKeyPart4());
		if (localKey > paramKey) {
			return 1;
		} else if (localKey < paramKey){
			return -1;
		}
		return 0;
	}
}
