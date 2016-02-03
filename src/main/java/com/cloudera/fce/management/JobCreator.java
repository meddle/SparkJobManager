package com.cloudera.fce.management;

public interface JobCreator {
	public void sendJob(Job job, String name);
	public void connect();
	public void disconnect();
}
