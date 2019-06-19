package com.beans.map.reduce.mapreduce;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.beans.map.reduce.io.KeyValue;

public class ReadHFileMapper extends Mapper<NullWritable, Cell, Text, KeyValue>{

	public static final String CONFIG_HBASE_TO_TABLE_COLUMN_MAP = "fullexport.hbase.to.hive.column.map";
	private Map<String, String> hbaseToHiveColumnMap;
	private Text textKey = new Text();
	private KeyValue mapValue = new KeyValue();
	
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
	
	@Override
	protected void map(NullWritable key, Cell cell,
			Mapper<NullWritable, Cell, Text, KeyValue>.Context context)
			throws IOException, InterruptedException {
		String columnFamily = Bytes.toString(CellUtil.cloneFamily(cell));
		String qualifier = Bytes.toString(CellUtil.cloneFamily(cell));
		String colName = this.hbaseToHiveColumnMap.get(columnFamily + ":" + qualifier);
		if(colName != null) {
			byte[] colValue = CellUtil.cloneValue(cell);
			String rowkey = Bytes.toString(CellUtil.cloneRow(cell));
			textKey.set(rowkey);
			mapValue.put(new Text(colName), new BytesWritable(colValue), new LongWritable(cell.getTimestamp()));
			context.write(textKey, mapValue);
		}
	}
}
