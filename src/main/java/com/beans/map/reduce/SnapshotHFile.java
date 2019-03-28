package com.beans.map.reduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.io.HFileLink;
import org.apache.hadoop.hbase.protobuf.generated.SnapshotProtos.SnapshotRegionManifest.StoreFile;
import org.apache.hadoop.hbase.regionserver.StoreFileInfo;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.snapshot.SnapshotReferenceUtil;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.log4j.Logger;

import com.beans.map.reduce.util.HBaseUtil;

public class SnapshotHFile {
	private static final Logger logger = Logger.getLogger(SnapshotHFile.class);
	
	public static String createSnapShop(){
		String zookeeperAddress = "";
		String zookeeperPort = "2181";
		String maxThreadsPerHtable = "50";
		String tableName = "ecitem:IM_ItemBase";
		String partitionValue = System.currentTimeMillis()+"";
		String snapShotName = "";
    	try {
    		HBaseUtil hbaseUtil = new HBaseUtil(zookeeperAddress, zookeeperPort, maxThreadsPerHtable);
    		snapShotName = hbaseUtil.createSnapshot(tableName, partitionValue);
        } catch (Exception e) {
            e.printStackTrace();
        } 
    	
    	return snapShotName;
	}
	
	public static List<String> getSnapshotFiles() throws IOException  {
		String snapshotName = createSnapShop();
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.rootdir", "hdfs:///hbase");
        config.set("dfs.permissions.enabled", "false");
        Set<String> columns = new HashSet<String>();
        String columnFamilies = "BaseInfo,ImageInfo";
        for(String c : columnFamilies.split(",")) {
        	columns.add(c);
        }

		List<Path> list = getSnapshotFiles(config, snapshotName, columns);
		List<String> pathList = new ArrayList<String>();
		for(Path path: list) {
			pathList.add(path.toString());
			logger.info("[" + snapshotName + "]: " + path.toString());
		}
		
		return pathList;

	}
	
	/**
	 * 获取快照文件
	 */
	private static List<Path> getSnapshotFiles(Configuration config, String snapshotName, Set<String> columnFamilies) throws IOException {
		Path rootPath = FSUtils.getRootDir(config);
		Path snapshotDir = SnapshotDescriptionUtils.getCompletedSnapshotDir(snapshotName, rootPath);
		FileSystem snapshotFs = FileSystem.get(snapshotDir.toUri(), config);

		final List<Path> snapshotReferences = new ArrayList<Path>();
		SnapshotReferenceUtil.visitReferencedFiles(config, snapshotFs, snapshotDir,
			new SnapshotReferenceUtil.SnapshotVisitor() {
				@Override
				public void logFile(String server, String logfile) throws IOException {
					// snapshotReferences.add(new Path(server, logfile));
				}
				@Override
				public void storeFile(HRegionInfo region, String family, StoreFile hfile) throws IOException {
					String regionName = region.getEncodedName();
					String hfileName = hfile.getName();
					Path path = HFileLink.createPath(region.getTable(), regionName, family, hfileName);
					snapshotReferences.add(path);
				}
			});

		List<Path> list = new ArrayList<Path>();
		FileSystem hbaseFs = FileSystem.get(rootPath.toUri(), config);
		Set<String> searchColumnFamiliesSet = new HashSet<String>();
		for(String s : columnFamilies) {
			searchColumnFamiliesSet.add("/" + s + "/");
		}
		String[] searchColumnFamilies = searchColumnFamiliesSet.toArray(new String[searchColumnFamiliesSet.size()]);
		for (Path path : snapshotReferences) {
			if (HFileLink.isHFileLink(path) || StoreFileInfo.isReference(path) || HFileLink.isBackReferencesDir(path)) {
				HFileLink link = HFileLink.buildFromHFileLinkPattern(config, path);
				FileStatus status = link.getFileStatus(hbaseFs);
				if (status != null) {
					String hfilePath = status.getPath().toString();
					if (StringUtils.indexOfAny(hfilePath, searchColumnFamilies) != -1) {
						list.add(status.getPath());
					}
				}
			}
		}

		return list;
	}
}
