package com.cloudera.fce.management;

public interface JobManager {
	public Job getJob();
	public Job waitForJob();
	public void markComplete(Job job);

}
