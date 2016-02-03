package com.cloudera.fce.management;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Map;

import org.apache.log4j.Logger;

import kafka.serializer.Encoder;
import kafka.utils.VerifiableProperties;

public class JobEncoder implements Encoder<Job>,org.apache.kafka.common.serialization.Serializer<Job> {
	
	public JobEncoder() {
		
	}
	
	public JobEncoder(VerifiableProperties verifiableProperties) {
        /* This constructor must be present for successful compile. */
    }

	@Override
	public byte[] toBytes(Job job){
		final Logger logger = Logger.getLogger(JobEncoder.class);
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream oos = null;
		try {
			oos = new ObjectOutputStream(bos);
		} catch (IOException e) {
			 logger.error(String.format("Job Encoding failed for object: %s", job.getClass().getName()), e);
		}
        try {
			oos.writeObject(job);
			return bos.toByteArray();
		} catch (IOException e) {
			logger.error(String.format("Job Encoding failed for object: %s", job.getClass().getName()), e);
		}
        
        return "".getBytes();
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void configure(Map arg0, boolean arg1) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public byte[] serialize(String arg0, Job obj) {
		final Logger logger = Logger.getLogger(JobEncoder.class);
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream oos = null;
		try {
			oos = new ObjectOutputStream(bos);
		} catch (IOException e) {
			 logger.error(String.format("Job Encoding failed for object: %s", obj.getClass().getName()), e);
		}
        try {
			oos.writeObject(obj);
			return bos.toByteArray();
		} catch (IOException e) {
			logger.error(String.format("Job Encoding failed for object: %s", obj.getClass().getName()), e);
		}
        
        return "".getBytes();
	}

}
