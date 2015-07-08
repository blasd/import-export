package com.gigaspaces.tools.importexport.serial;

import java.io.Serializable;

public class Audit extends SerialList implements Serializable {
	
	private Integer partition = 0;

	private static final long serialVersionUID = 3485223374395266543L;

	public Audit() {

	}
	
	public Audit(Integer partition) {
		this.partition = partition;
	}

	@Override
	public boolean add(String e) {
		String p = (partition > 0 ? "/pid-" + partition : "");
		return super.add("(tid-" + Thread.currentThread().getId()  + p + ") : " + e);
	}

	@Override
	public void add(int index, String element) {

		super.add(index, Thread.currentThread().getId() + " : " + element);
	}

	public Integer getPartition() {
		return partition;
	}

	public void setPartition(Integer partition) {
		this.partition = partition;
	}

}
