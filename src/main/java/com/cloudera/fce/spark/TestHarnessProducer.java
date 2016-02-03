package com.cloudera.fce.spark;

import com.cloudera.fce.management.Job;

public class TestHarnessProducer {

	public static void main(String[] args) {
		SparkJobCreator sjc = new SparkJobCreator("dayrhewtrp005:9092,dayrhewtrp004:9092");
		Job job = new Job();
		job.setField("xmlspec", "<xml><test>data here</data></xml>");
		sjc.sendJob(job, "small_reports");
		sjc.disconnect();
	}
}

