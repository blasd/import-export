package com.gigaspaces.tools.importexport.serial;

import java.io.Serializable;

public class Audit extends SerialList implements Serializable {
	
	private Integer partition = 0;

	private static final long serialVersionUID = 3485223374395266543L;
    private String className;
    private long start;
    private int count;
    private long stop;
    private String fileName;

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

    public void setClassName(String className) {
        this.className = className;
    }

    public String getClassName() {
        return className;
    }

    public void start() {
        this.start = System.currentTimeMillis();
    }

    public void setCount(int count) {
        this.count = count;
    }

    public int getCount() {
        return count;
    }

    public void stop() {
        this.stop = System.currentTimeMillis();
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public String getFileName() {
        return fileName;
    }

    public long getTime() {
        return stop - start;
    }
}
