package com.cloudera.fce.spark;

import com.cloudera.fce.management.Job;

public class TestHarnessConsumer {

	public static void main(String[] args) {
		SparkJobManager sjm = new SparkJobManager("small_reports", "group1","dayrhewtrp002:2181");
		Job rJob = sjm.getJob();
		System.out.println(rJob.getField("xmlspec"));
	}
}
