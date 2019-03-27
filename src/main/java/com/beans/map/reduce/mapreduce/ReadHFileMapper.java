package com.beans.map.reduce.mapreduce;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.beans.map.reduce.io.KeyValue;

public class ReadHFileMapper extends Mapper<NullWritable, Cell, Text, KeyValue>{

	private Map<String, String> hbaseToHiveColumnMap;
	
	@Override
	protected void map(NullWritable key, Cell value,
			Mapper<NullWritable, Cell, Text, KeyValue>.Context context)
			throws IOException, InterruptedException {
		String columnFamily = Bytes.toString(CellUtil.cloneFamily(value));
		String qualifier = Bytes.toString(CellUtil.cloneFamily(value));
		String colName = 
	}

	@Override
	protected void setup(
			Mapper<NullWritable, Cell, Text, KeyValue>.Context context)
			throws IOException, InterruptedException {
		this.hbaseToHiveColumnMap = new HashMap<String, String>();
		this.hbaseToHiveColumnMap.put("BaseInfo:ItemNumber", "");
		this.hbaseToHiveColumnMap.put("BaseInfo:SubCategoryCode", "");
		this.hbaseToHiveColumnMap.put("BaseInfo:ImageName", "");
		this.hbaseToHiveColumnMap.put("BaseInfo:HumanRating", "");
	}

}
