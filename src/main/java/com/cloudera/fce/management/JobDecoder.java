package com.cloudera.fce.management;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;

import org.apache.log4j.Logger;

import kafka.serializer.Decoder;

public class JobDecoder implements Decoder<Job> {

	@Override
	public Job fromBytes(byte[] bytes) {
		ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
		final Logger logger = Logger.getLogger(JobEncoder.class);
        ObjectInputStream ois = null;
		try {
			ois = new ObjectInputStream(bis);
		} catch (IOException e1) {
			logger.error("Job DEcoding failed for object");
		}
        try {
			return (Job) ois.readObject();
		} catch (ClassNotFoundException | IOException e) {
			logger.error("Job DEcoding failed for object");
		}
		return null;
	}

}
