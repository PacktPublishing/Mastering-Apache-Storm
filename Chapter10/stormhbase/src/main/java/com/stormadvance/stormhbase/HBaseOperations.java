package com.stormadvance.stormhbase;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseOperations implements Serializable{

	private static final long serialVersionUID = 1L;

	// Instance of Hadoop Cofiguration class
	Configuration conf = new Configuration();
	
	HTable hTable = null;
	
	public HBaseOperations(String tableName, List<String> ColumnFamilies,
			List<String> zookeeperIPs, int zkPort) {
		conf = HBaseConfiguration.create();
		StringBuffer zookeeperIP = new StringBuffer();
		// Set the zookeeper nodes
		for (String zookeeper : zookeeperIPs) {
			zookeeperIP.append(zookeeper).append(",");
		}
		zookeeperIP.deleteCharAt(zookeeperIP.length() - 1);

		conf.set("hbase.zookeeper.quorum", zookeeperIP.toString());
		
		// Set the zookeeper client port
		conf.setInt("hbase.zookeeper.property.clientPort", zkPort);
		// call the createTable method to create a table into HBase.
		createTable(tableName, ColumnFamilies);
		try {
			// initilaize the HTable. 
			hTable = new HTable(conf, tableName);
		} catch (IOException e) {
			throw new RuntimeException("Error occure while creating instance of HTable class : " + e);
		}
	}

	/**
	 * This method create a table into HBase
	 * 
	 * @param tableName
	 *            Name of the HBase table
	 * @param ColumnFamilies
	 *            List of column famallies
	 * 
	 */
	public void createTable(String tableName, List<String> ColumnFamilies) {
		HBaseAdmin admin = null;
		try {
			admin = new HBaseAdmin(conf);
			// Set the input table in HTableDescriptor
			HTableDescriptor tableDescriptor = new HTableDescriptor(
					Bytes.toBytes(tableName));
			for (String columnFamaliy : ColumnFamilies) {
				HColumnDescriptor columnDescriptor = new HColumnDescriptor(
						columnFamaliy);
				// add all the HColumnDescriptor into HTableDescriptor
				tableDescriptor.addFamily(columnDescriptor);
			}
			/* execute the creaetTable(HTableDescriptor tableDescriptor) of HBaseAdmin
			 * class to createTable into HBase.
			*/ 
			admin.createTable(tableDescriptor);
			admin.close();
			
		}catch (TableExistsException tableExistsException) {
			System.out.println("Table already exist : " + tableName);
			if(admin != null) {
				try {
				admin.close(); 
				} catch (IOException ioException) {
					System.out.println("Error occure while closing the HBaseAdmin connection : " + ioException);
				}
			}
			
		}catch (MasterNotRunningException e) {
			throw new RuntimeException("HBase master not running, table creation failed : ");
		} catch (ZooKeeperConnectionException e) {
			throw new RuntimeException("Zookeeper not running, table creation failed : ");
		} catch (IOException e) {
			throw new RuntimeException("IO error, table creation failed : ");
		}
	}
	
	/**
	 * This method insert the input record into HBase.
	 * 
	 * @param record
	 *            input record
	 * @param rowId
	 *            unique id to identify each record uniquely.
	 */
	public void insert(Map<String, Map<String, Object>> record, String rowId) {
		try {
		Put put = new Put(Bytes.toBytes(rowId));		
		for (String cf : record.keySet()) {
			for (String column: record.get(cf).keySet()) {
				put.add(Bytes.toBytes(cf), Bytes.toBytes(column), Bytes.toBytes(record.get(cf).get(column).toString()));
			} 
		}
		hTable.put(put);
		}catch (Exception e) {
			throw new RuntimeException("Error occure while storing record into HBase");
		}
		
	}

	public static void main(String[] args) {
		List<String> cFs = new ArrayList<String>();
		cFs.add("cf1");
		cFs.add("cf2");

		List<String> zks = new ArrayList<String>();
		zks.add("192.168.41.122");
		Map<String, Map<String, Object>> record = new HashMap<String, Map<String,Object>>();
		
		Map<String, Object> cf1 = new HashMap<String, Object>();
		cf1.put("aa", "1");
		
		Map<String, Object> cf2 = new HashMap<String, Object>();
		cf2.put("bb", "1");
		
		record.put("cf1", cf1);
		record.put("cf2", cf2);
		
		HBaseOperations hbaseOperations = new HBaseOperations("tableName", cFs, zks, 2181);
		hbaseOperations.insert(record, UUID.randomUUID().toString());

	}
}
