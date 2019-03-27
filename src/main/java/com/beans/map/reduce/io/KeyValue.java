package com.beans.map.reduce.io;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class KeyValue implements Serializable, Cloneable, WritableComparable<KeyValue> {
	private static final long serialVersionUID = 6816606933757056160L;
	private Text key;
	private BytesWritable value;
	private LongWritable timestamp;

	public KeyValue() {
		this.put(new Text(), new BytesWritable(), new LongWritable());
	}

	public void put(Text key, BytesWritable value, LongWritable timestamp) {
		this.key = key;
		this.value = value;
		this.timestamp = timestamp;
	}

	public Text getKey() {
		return key;
	}
	public void setKey(Text key) {
		this.key = key;
	}
	public BytesWritable getValue() {
		return value;
	}
	public void setValue(BytesWritable value) {
		this.value = value;
	}
	public LongWritable getTimestamp() {
		return timestamp;
	}
	public void setTimestamp(LongWritable timestamp) {
		this.timestamp = timestamp;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		this.key.write(out);
		this.value.write(out);
		this.timestamp.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.key.readFields(in);
		this.value.readFields(in);
		this.timestamp.readFields(in);
	}

	@Override
	public int compareTo(KeyValue o) {
		int compareKey = this.key.compareTo(o.getKey());
		if(compareKey == 0) {
			int compareTimestamp = this.timestamp.compareTo(o.getTimestamp());
			if(compareTimestamp == 0) {
				return this.value.compareTo(o.getValue());
			} else {
				return compareTimestamp;
			}
		} else {
			return compareKey;
		}
	}

	@Override
	public boolean equals(Object o) {
		if(o == null || !(o instanceof KeyValue)) {
			return false;
		}
		KeyValue keyValue = (KeyValue)o;
		if(this.key.equals(keyValue.getKey())
			&& this.value.equals(keyValue.getValue())
			&& this.timestamp.equals(keyValue.getTimestamp())) {
			return true;
		} else {
			return false;
		}
	}
}
