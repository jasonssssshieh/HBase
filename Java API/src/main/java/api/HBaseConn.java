package com.imooc.bigdata.hbase.api;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;

public class HBaseConn{
	private static final HBaseConn INSTANCE = new HBaseConn();
	private static Configuration configuration;
	private static Connection connection; 
	
	private HBaseConn(){
		try{
			if(configuration == null){
				configuration = HBaseConfiguration.create();
				configuration.set("hbase.zookeeper.quorum", "localhost:2181");
			}
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	privtae Connection getConnection(){
		if(connection == null || connection.isClosed()){
			try{
				connection = ConnectionFactory.createConnection(configuration);
			} catch (Exception e){
				e.printStackTrace();
			}
		}
		return connection;
	}
	
	public static Connection getHBase(){
		return INSTANCE.getConnection();
	}
	
	public static Table getTable(String tableName){
		return INSTANCE.getConnection().getTable(TableName.valueOf(tableName));
	}
	
	public static void closeConn(){
		if(connection != null){
			try{
				connection.close();
			} catch(IOException ioe){
				ioe.printStackTrace();
			}
		}
	}
}