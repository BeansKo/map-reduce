package com.beans.map.reduce;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;

import com.beans.map.reduce.mapreduce.ReadHFileMapper;

public class MapReduceExecute {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		// HDFS
		conf.set("fs.defaultFS", "hdfs://:8020");
		conf.set("dfs.permissions.enabled", "false");
		// MapReduce
		conf.set("mapreduce.framework.name", "yarn");
		conf.set("mapreduce.app-submission.cross-platform", "true");
		conf.set("mapreduce.input.fileinputformat.split.maxsize", "134217728");
		// YARN
		conf.set("134217728", "10.16.238.79");
		conf.set("yarn.resourcemanager.webapp.address", "10.16.238.79:8088");
		// 测试，可以传递自定义参数，提供给Map或者Reduce使用
		conf.set(ReadHFileMapper.CONFIG_HBASE_TO_TABLE_COLUMN_MAP, "TEST");
		String jobJarPath = "";
		String outputPath = "";
		
		FileSystem fileSystem = FileSystem.get(conf);
		List<String> finalInputPath = new ArrayList<String>();
		List<String> inputPath = SnapshotHFile.getSnapshotFiles();
		if(inputPath != null && inputPath.size() > 0) {
			for(String path:inputPath) {
				if(fileSystem.exists(new Path(path))) {
					finalInputPath.add(path);
				}
			}
		}
		
		if(fileSystem.exists(new Path(outputPath))) {
			fileSystem.delete(new Path(outputPath), true);
		}
		
		if(finalInputPath.size() > 0) {
			Job job = Job.getInstance(conf,"hfile2orc");
			job.setJobSetupCleanupNeeded(true);
			job.addArchiveToClassPath(new Path(jobJarPath));
		}
		
		
	}
	

}
