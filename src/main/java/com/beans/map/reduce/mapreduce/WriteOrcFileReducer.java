package com.beans.map.reduce.mapreduce;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.orc.OrcConf;
import org.apache.orc.TypeDescription;
import org.apache.orc.mapred.OrcStruct;

import com.beans.map.reduce.io.KeyValue;

public class WriteOrcFileReducer extends Reducer<Text,KeyValue, NullWritable, OrcStruct>{
	public static final String CONFIG_HIVE_TABLE_PARTITION = "fullexport.hive.table.partition";
	public static final String CONFIG_HIVE_TABLE_PARTITION_COLUMN = "fullexport.hive.table.partitionColumn";
	private List<String> columnNames;
	private TypeDescription schema;
	private String partition;
	private String partitionColumn;
	private final NullWritable nullKey = NullWritable.get();
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		this.partition = context.getConfiguration().get(CONFIG_HIVE_TABLE_PARTITION);
		this.partitionColumn = context.getConfiguration().get(CONFIG_HIVE_TABLE_PARTITION_COLUMN);
		String schemaStr = context.getConfiguration().get(OrcConf.MAPRED_OUTPUT_SCHEMA.getAttribute());
		schemaStr = schemaStr.substring(0, schemaStr.lastIndexOf(">")) + "," +partitionColumn + ":string>";
		this.schema = TypeDescription.fromString(schemaStr);
		this.columnNames = schema.getFieldNames();
	}
	
	@Override
	protected void reduce(Text key, Iterable<KeyValue> value,Context arg2)
			throws IOException, InterruptedException {
		OrcStruct outStruct = new OrcStruct(schema);
		Map<String, Long> colNameToTimeStamp = new HashMap<String, Long>();
		Iterator<KeyValue> it = value.iterator();
		KeyValue map = null;
		while (it.hasNext()) {
			map = it.next();
			String colName = map.getKey().toString();
			if(this.columnNames.contains(colName)) {
				Long timestamp = colNameToTimeStamp.get(colName);
				if(timestamp == null || timestamp < map.getTimestamp().get()) {
					colNameToTimeStamp.put(colName, map.getTimestamp().get());
				}
			}
		}
	}



}
