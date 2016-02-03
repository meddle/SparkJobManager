package com.cloudera.fce.management;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.UUID;

import com.fasterxml.jackson.annotation.JsonProperty;

import kafka.serializer.Decoder;

import java.util.Map;

public class Job implements Serializable,Decoder<Job> {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String jobId = UUID.randomUUID().toString();
	@JsonProperty("fields") private Map<String,String> fields = new HashMap<String, String>();
	
	public String getJobId() {
		return jobId;
	}

	public void setJobId(String jobId) {
		this.jobId = jobId;
	}
	
	public void setField(String key, String value) {
		this.fields.put(key, value);
	}
	
	public String getField(String key) {
		return this.fields.get(key);
	}

	@Override
	public Job fromBytes(byte[] bytes) {
		ByteArrayInputStream in = new ByteArrayInputStream(bytes);
	    ObjectInputStream is = null;
		try {
			is = new ObjectInputStream(in);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    try {
			return (Job) is.readObject();
		} catch (ClassNotFoundException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    return null;
	}
}
