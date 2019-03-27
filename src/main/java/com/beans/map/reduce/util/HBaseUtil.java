package com.beans.map.reduce.util;

import org.apache.log4j.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

public class HBaseUtil {
	private static final Logger logger = Logger.getLogger(HBaseUtil.class);

	private String zookeeperAddress;
    private String zookeeperPort;
    private String maxThreadsPerHtable;

    public HBaseUtil(String zookeeperAddress, String zookeeperPort, String maxThreadsPerHtable) throws Exception {
		super();
		this.zookeeperAddress = zookeeperAddress;
		this.zookeeperPort = zookeeperPort;
		this.maxThreadsPerHtable = maxThreadsPerHtable;
	}

    public String createSnapshot(String tableName, String timestamp) throws Exception {
        try (Connection conn = createConnection()) {
            try (Admin admin = conn.getAdmin()) {
		        String snapshotName = getSnapshotName(tableName, timestamp);
		        logger.info(String.format("create snapshot name: %s.", snapshotName));
		        if (admin.listSnapshots(snapshotName).size() > 0) {
		            admin.deleteSnapshot(snapshotName);
		        }
		        admin.snapshot(snapshotName, TableName.valueOf(tableName));
		        return snapshotName;
            }
        }
    }

    public void deleteSnapshot(String snapshotName) throws Exception {
        try (Connection conn = createConnection()) {
            try (Admin admin = conn.getAdmin()) {
		        logger.info(String.format("delete snapshot name: %s.", snapshotName));
		        if (admin.listSnapshots(snapshotName).size() > 0) {
		            admin.deleteSnapshot(snapshotName);
		        }
            }
        }
    }

    private Connection createConnection() throws Exception {
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", zookeeperAddress);
        config.set("hbase.zookeeper.property.clientPort", zookeeperPort);
        config.set("hbase.htable.threads.max", maxThreadsPerHtable);
        return ConnectionFactory.createConnection(config);
    }

    /**
     * [a-zA-Z_0-9-.] 快照名规范
     */
    private String getSnapshotName(String tableName, String timestamp) {
        return String
            .format("DataFeed_%s_%s", tableName, timestamp)
            .replaceAll(":", "_");
    }
    
    public static void main(String[] args) {
    	
    }
}
