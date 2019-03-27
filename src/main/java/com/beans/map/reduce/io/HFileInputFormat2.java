package com.beans.map.reduce.io;


import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFile.Reader;
import org.apache.hadoop.hbase.io.hfile.HFileBlockIndex.BlockIndexReader;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.primitives.Longs;

/**
 * Simple MR input format for HFiles. This code was borrowed from Apache Crunch
 * project. Updated to the recent version of HBase.
 */
public class HFileInputFormat2 extends CombineFileInputFormat<NullWritable, Cell> {

	private static final Logger LOG = LoggerFactory.getLogger(HFileInputFormat2.class);
	private static final String SPLIT_SEPARATOR = "^_^";

	/**
	 * File filter that removes all "hidden" files. This might be something worth
	 * removing from a more general purpose utility; it accounts for the presence of
	 * metadata files created in the way we're doing exports.
	 */
	static final PathFilter HIDDEN_FILE_FILTER = new PathFilter() {
		@Override
		public boolean accept(Path p) {
			String name = p.getName();
			return !name.startsWith("_") && !name.startsWith(".");
		}
	};

	/**
	 * Record reader for HFiles.
	 */
	private static class HFileRecordReader extends RecordReader<NullWritable, Cell> {

		private Reader in;
		protected Configuration conf;
		private HFileScanner scanner;

		/**
		 * A private cache of the key value so it doesn't need to be loaded twice from
		 * the scanner.
		 */
		private Cell value = null;
		private long count;
		private boolean seeked = false;
		private boolean hasNext = false;

		private String firstKey = null;
		private String startRow = null;
		private byte[] startRowByte = null;
		private String endRow = null;
		private byte[] endRowByte = null;

		@SuppressWarnings("unused")
		public HFileRecordReader(CombineFileSplit split, TaskAttemptContext context, Integer index) throws IOException {
			conf = context.getConfiguration();
			FileSystem fs = FileSystem.get(conf);
			Path path = initStartRowAndEndRow(split.getPath(index));
			boolean exist = fs.exists(path);
			if (exist && in == null) {
				try {
					in = HFile.createReader(fs, path, new CacheConfig(conf), conf);
					in.loadFileInfo();
					firstKey = Bytes.toString(in.getFirstRowKey());
					scanner = in.getScanner(false, false);
					return;
				} catch(IOException e) {
					LOG.warn("there is still a window the file will be removed before we create thre reader " + e.getMessage()); 
				}
			}

			LOG.info("there is no hfile mapping the path " + path); 
			Path realFile = findRealFile(fs, new Path("/hbase/archive"), path.getName());
			if(!fs.exists(realFile)) {
				throw new IOException("we could not find the hfile " + path); 
			}
			LOG.info("we use the path " + realFile + " instead of " + path); 

			in = HFile.createReader(fs, realFile, new CacheConfig(conf), conf);
			in.loadFileInfo();
			firstKey = Bytes.toString(in.getFirstRowKey());
			scanner = in.getScanner(false, false);
		}

		private Path initStartRowAndEndRow(Path path) {
			String location = path.toString();
			int splitIndex = location.indexOf(SPLIT_SEPARATOR);
			if(splitIndex == -1) {
				this.startRow = null;
				this.startRowByte = null;
				this.endRow = null;
				this.endRowByte = null;
				return path;
			} else {
				String[] startAndEnd = location.substring(splitIndex + 3).split("\\-");
				this.startRow = startAndEnd[0];
				this.startRowByte = Bytes.toBytes(this.startRow);
				this.endRow = startAndEnd[1];
				this.endRowByte = Bytes.toBytes(this.endRow);
				location = location.substring(0, splitIndex);
				return new Path(location);
			}
		}

		private Path findRealFile(FileSystem fs, Path root, String name) throws IOException {
		    RemoteIterator<LocatedFileStatus> fileStatusListIterator = fs.listFiles(root, true);
		    while(fileStatusListIterator.hasNext()){
		        LocatedFileStatus fileStatus = fileStatusListIterator.next();
		        if(fileStatus.getPath().getName().equals(name)) {
		        	return fileStatus.getPath();
		        }
		    }
		    throw new IOException("could not find the hfile from " + name); 
		}

		@Override
		public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
			// do nothing
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			try {
				if (!seeked) {
					LOG.info("Seeking to start");
					if(startRow != null && !startRow.equals(firstKey)) {
						hasNext = scanner.seekTo(KeyValueUtil.createFirstOnRow(startRowByte)) != -1;
					} else {
						hasNext = scanner.seekTo();
					}
					seeked = true;
				} else {
					hasNext = scanner.next();
				}
			} catch (FileNotFoundException e) {
				LOG.error("scan hfile error", e);
				hasNext = false;
			}
			if (!hasNext) {
				return false;
			}
			value = scanner.getKeyValue();
			if(endRow != null) {
				int result = CellComparator.compareRows(
						value.getRowArray(), value.getRowOffset(), value.getRowLength(),
						endRowByte, 0, endRowByte.length);
				if (result > 0) {
					return false;
				}
			}
			count++;
			return true;
		}

		@Override
		public NullWritable getCurrentKey() throws IOException, InterruptedException {
			return NullWritable.get();
		}

		@Override
		public Cell getCurrentValue() throws IOException, InterruptedException {
			return value;
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			// This would be inaccurate if KVs are not uniformly-sized or we have performed
			// a seek to
			// the start row, but better than nothing anyway.
			return hasNext ? 1.0f * count / in.getEntries() : 1.0f;
		}

		@Override
		public void close() throws IOException {
			if (in != null) {
				in.close();
				in = null;
			}
		}
	}

	@Override
	protected List<FileStatus> listStatus(JobContext job) throws IOException {
		List<FileStatus> result = new ArrayList<FileStatus>();

		// Explode out directories that match the original FileInputFormat filters
		// since HFiles are written to directories where the
		// directory name is the column name
		for (FileStatus status : super.listStatus(job)) {
			if (status.isDirectory()) {
				FileSystem fs = status.getPath().getFileSystem(job.getConfiguration());
				for (FileStatus match : fs.listStatus(status.getPath(), HIDDEN_FILE_FILTER)) {
					result.add(match);
				}
			} else {
				result.add(status);
			}
		}
		return result;
	}

	@Override
	public RecordReader<NullWritable, Cell> createRecordReader(InputSplit split, TaskAttemptContext context)
			throws IOException {
		return new CombineFileRecordReader<NullWritable, Cell>
	      ((CombineFileSplit)split, context, HFileRecordReader.class);
	}

	@Override
	protected boolean isSplitable(JobContext context, Path filename) {
		// This file isn't splittable.
		return false;
	}

	@Override
	public List<InputSplit> getSplits(JobContext job) throws IOException {
		Configuration conf = job.getConfiguration();
		long blockSize = conf.getLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, DFSConfigKeys.DFS_BLOCK_SIZE_DEFAULT);
		long maxSize = conf.getLong("mapreduce.input.fileinputformat.split.maxsize", blockSize);
		if(maxSize < blockSize) {
			maxSize = blockSize;
		}
		FileSystem fs = FileSystem.get(conf);
		List<InputSplit> splits = super.getSplits(job);

		LOG.info("[OLD] number of splits: " + splits.size());
		List<CombineFileSplit> largeSplits = new ArrayList<CombineFileSplit>();
		for(InputSplit split : splits) {
			CombineFileSplit combineSplit = (CombineFileSplit)split;
			LOG.debug("SPLIT_DETAIL: " + splitToString(combineSplit, maxSize));
			if(combineSplit.getLength() > maxSize) {
				largeSplits.add(combineSplit);
			}
		}
		if(largeSplits.size() > 0) {
			int addedCount = 0;
			List<CombineFileSplit> trailingSplit = new ArrayList<CombineFileSplit>();
			for(CombineFileSplit combineSplit : largeSplits) {
				splits.remove(combineSplit);
				List<InputSplit> splitAgain = reSplit(combineSplit, fs, conf, maxSize, trailingSplit);
				splits.addAll(splitAgain);
				addedCount += splitAgain.size() - 1;
			}
			LOG.info("Added from large split: " + addedCount);
			if(trailingSplit.size() > 0) {
				List<InputSplit> trailing = mergeTrailingSplits(trailingSplit, maxSize);
				splits.addAll(trailing);
				LOG.info("Added from trailing split: " + trailing.size());
			}
		}
		LOG.info("[NEW] number of splits: " + splits.size());
		return splits;
	}

	/**
	 * Re-Split split because of HFile is not splittable
	 * @param combineSplit input-split
	 * @param fs filesystem instance
	 * @param conf hadoop configuration
	 * @param maxSize max split size in byte
	 * @param trailingSplit list for collect trailing smaller split
	 * @return splits
	 * @throws IOException
	 */
	private List<InputSplit> reSplit(CombineFileSplit combineSplit, FileSystem fs, Configuration conf, long maxSize, List<CombineFileSplit> trailingSplit) throws IOException {
		if(combineSplit.getNumPaths() == 1) {
			return splitOne(combineSplit.getPath(0), combineSplit.getLocations(), fs, conf, maxSize);
		} else {
			List<InputSplit> splitResult = new ArrayList<InputSplit>();
			List<FileStatus> files = Arrays.asList(fs.listStatus(combineSplit.getPaths()));

			String[] locations = combineSplit.getLocations();

			List<Path> paths = new ArrayList<Path>();
			List<Long> startoffset = new ArrayList<Long>();
			List<Long> lengths = new ArrayList<Long>();

			long totalSize = 0;
			for(FileStatus file : files) {
				if(file.getLen() > maxSize) {
					splitResult.addAll(splitOne(file.getPath(), locations, fs, conf, maxSize));
				} else {
					totalSize += file.getLen();
					paths.add(file.getPath());
					startoffset.add(0L);
					lengths.add(file.getLen());
					if(totalSize >= maxSize) {
						splitResult.add(new CombineFileSplit(
							paths.toArray(new Path[paths.size()]),
							Longs.toArray(startoffset),
							Longs.toArray(lengths),
							locations));
						totalSize = 0;
						paths.clear();
						startoffset.clear();
						lengths.clear();
					}
				}
			}
			if(paths.size() > 0) {
				trailingSplit.add(new CombineFileSplit(
					paths.toArray(new Path[paths.size()]),
					Longs.toArray(startoffset),
					Longs.toArray(lengths),
					locations));
			}

			return splitResult;
		}
	}

	/**
	 * Split one large HFile
	 * @param path file that need to be splitted
	 * @param locations where this input-split resides
	 * @param fs filesystem instance
	 * @param conf hadoop configuration
	 * @param maxSize max split size in byte
	 * @return splits
	 * @throws IOException
	 */
	private List<InputSplit> splitOne(Path path, String[] locations, FileSystem fs, Configuration conf, long maxSize) throws IOException {
		String pathStr = path.toString();
		long fileSize = fs.getFileStatus(path).getLen();
		long[] startoffset = new long[] {0};
		long[] lengths = new long[] {fileSize};

		List<InputSplit> splitResult = new ArrayList<InputSplit>();

		try(Reader in = HFile.createReader(fs, path, new CacheConfig(conf), conf)) {
			String firstKey = Bytes.toString(in.getFirstRowKey());
			String lastKey = Bytes.toString(in.getLastRowKey());

			// split ranges (start row -> end row)
			Map<String, String> ranges = new LinkedHashMap<String, String>();
			if(fileSize < maxSize * 2) {
				ranges.put(firstKey, lastKey);
			} else if(fileSize >= maxSize * 2 && fileSize < maxSize * 3) {
				String midKey = Bytes.toString(CellUtil.cloneRow(new KeyValue.KeyOnlyKeyValue(in.midkey())));
				ranges.put(firstKey, midKey);
				ranges.put(midKey, lastKey);
			} else {
				long splitNum = fileSize / maxSize;
				BlockIndexReader indexReader = in.getDataBlockIndexReader();
				int rootBlockCount = indexReader.getRootBlockCount();
				int blockPerSplit = (int) ((rootBlockCount + (splitNum - rootBlockCount % splitNum)) / splitNum);
				List<String> splitPoint = new ArrayList<String>();
				splitPoint.add(firstKey);
				for(int i=blockPerSplit; i<rootBlockCount; i=i+blockPerSplit) {
					splitPoint.add(Bytes.toString(CellUtil.cloneRow(new KeyValue.KeyOnlyKeyValue(indexReader.getRootBlockKey(i)))));
				}
				if(!splitPoint.contains(lastKey)) {
					splitPoint.add(lastKey);
				}
				for(int i=0; i<splitPoint.size()-1; i++) {
					ranges.put(splitPoint.get(i), splitPoint.get(i+1));
				}
			}

			LOG.info("SplitNum=" + ranges.size() + ", Entries=" + in.getEntries() + ", FileSize=" + humanFileSize(fileSize) + ", File=" + pathStr);

			// create new input splits
			if(ranges.size() == 1) {
				splitResult.add(new CombineFileSplit(new Path[] {path}, startoffset, lengths, locations));
			} else {
				String startRow = null;
				String endRow = null;
				for(Map.Entry<String, String> entry : ranges.entrySet()) {
					startRow = entry.getKey();
					endRow = entry.getValue();
					Path[] files = new Path[] {new Path(pathStr + SPLIT_SEPARATOR + startRow + "-" + endRow)};
					splitResult.add(new CombineFileSplit(files, startoffset, lengths, locations));
				}
			}

			return splitResult;
		}
	}

	private List<InputSplit> mergeTrailingSplits(List<CombineFileSplit> trailingSplit, long maxSize) throws IOException	{
		List<InputSplit> splits = new ArrayList<InputSplit>();
		long totalSize = 0;
		List<Path> paths = new ArrayList<Path>();
		List<Long> startoffset = new ArrayList<Long>();
		List<Long> lengths = new ArrayList<Long>();
		Set<String> locations = new HashSet<String>();
		for(CombineFileSplit split : trailingSplit) {
			totalSize += split.getLength();
			paths.addAll(Arrays.asList(split.getPaths()));
			startoffset.addAll(Longs.asList(split.getStartOffsets()));
			lengths.addAll(Longs.asList(split.getLengths()));
			locations.addAll(Arrays.asList(split.getLocations()));
			if(totalSize >= maxSize) {
				splits.add(new CombineFileSplit(
						paths.toArray(new Path[paths.size()]),
						Longs.toArray(startoffset),
						Longs.toArray(lengths),
						locations.toArray(new String[locations.size()])));
				totalSize = 0;
				paths.clear();
				startoffset.clear();
				lengths.clear();
				locations.clear();
			}
		}
		if(paths.size() > 0) {
			splits.add(new CombineFileSplit(
				paths.toArray(new Path[paths.size()]),
				Longs.toArray(startoffset),
				Longs.toArray(lengths),
				locations.toArray(new String[locations.size()])));
		}
		return splits;
	}

	private String splitToString(CombineFileSplit combineSplit, long maxSize) {
		StringBuffer buffer = new StringBuffer();
		for(int i=0; i<combineSplit.getNumPaths(); i++) {
			buffer.append(",").append(humanFileSize(combineSplit.getLength(i)));
		}
		return "total: " + humanFileSize(combineSplit.getLength()) + ", largeSplit: " + (combineSplit.getLength() > maxSize) + ", files: " + (buffer.length() > 0 ? buffer.substring(1) : "");
	}

	private String humanFileSize(long bytes) {
		int thresh = 1024;
		if (Math.abs(bytes) < thresh) {
			return bytes + "B";
		}
		String[] units = new String[] { "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB" };
		int u = -1;
		double humanSize = (double) bytes;
		do {
			humanSize /= thresh;
			++u;
		} while (Math.abs(humanSize) >= thresh && u < units.length - 1);
		return String.format("%.2f%s", humanSize, units[u]);
	}

}
