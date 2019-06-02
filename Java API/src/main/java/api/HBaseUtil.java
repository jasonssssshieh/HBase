 package com.imooc.bigdata.hbase.api;
 import java.io.IOException;
 import java.util.*;
 
 import org.apache.hadoop.hbase.HColumnDescriptor;
 import org.apache.hadoop.hbase.HTableDescriptor;
 import org.apache.hadoop.hbase.TableName;
 import org.apache.hadoop.hbase.client.Delete;
 import org.apache.hadoop.hbase.client.Get;
 import org.apache.hadoop.hbase.client.HBaseAdmin;
 import org.apache.hadoop.hbase.client.Put;
 import org.apache.hadoop.hbase.client.Result;
 import org.apache.hadoop.hbase.client.ResultScanner;
 import org.apache.hadoop.hbase.client.Scan;
 import org.apache.hadoop.hbase.client.Table;
 import org.apache.hadoop.hbase.filter.FilterList;
 import org.apache.hadoop.hbase.util.Bytes;
 import org.omg.CORBA.PUBLIC_MEMBER;
 /*
Constant to define a public member in the ValueMember class.
PUBLIC_MEMBER is one of the two constants of typedef Visibility used in the interface repository to identify visibility of a ValueMember type. The other constant is PRIVATE_MEMBER.
 */
 
 
 public class HBaseUtil{
	/**
	create hbase table
	@para tablename
	@para cfs 列族的数组
	@return success or not
	*/
	public static class boolean createTable(String tableName, String[] cfs){
		try(HBaseAdmin admin = (HBaseAdmin)HBaseConn.getHBaseConn().getAdmin()){
			if(admin.tableExists(tableName)){
				return false;
			}
			HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
			Arrays.stream(cfs).forEach(cf->{
				HColumnDescriptor columnDescriptor = new HColumnDescriptor(cf);
				columnDescriptor.setMaxVersions(1);
				tableDescriptor.addFamily(columnDescriptor);
			});
			admin.createTable(tableDescriptor);
		} catch (Exception e){
			e.printStackTrace();
		}
		return true;
	}
	
	
	/*
	delete a table
	@para tablename
	return true or false for the deletion
	*/
	
	public static class boolean deleteTable(String tableName){
		try (HBaseAdmin admin = (HBaseAdmin)HBaseConn.getHBase.getHBaseConn().getAdmin()){
			admin.disableTable(tableName);
			admin.deleteTable(tableName);
		}catch(Exception e){
			e.printStackTrace();
		}
		return true;
	}
	
	/**
	insert a row
	@para rowkey
	@para cfName
	@para qualifier 列标识
	@para data
	@return 
	*/

	public static boolean putRow(String tableName, String rowKey, String cfName, String qualifier, String data){
		try(Table table = HBaseConn.getTable(tableName)){
			Put put = new Put(Bytes.toBytes(rowKey));
			put.addColumn(Bytes.toBytes(cfName), Bytes.toBytes(qualifier), Bytes.toBytes(data));
			table.put(put);
		}catch(IOException ioe){
			ioe.printStackTrace();
		}
		return true;
	}
 
	/**
	批量插入数据 
	*/
	
	public static boolean putRows(String tableName, List<put> puts){
		try(Table table = HBase.getTable(tableName)){
			table.put(puts);
		}catch(IOException ioe){
			ioe.printStackTrace();
		}
		return true;
	}
	
	/**
	获得单条数据
	@param tableName
	@param rowKey
	*/
	public static Result getRow(String tableName, String rowKey){
		try(Table table = HBase.getTable(tableName)){
			Get get = new Get(Bytes.toBytes(rowKey));
			return table.get(get);
		}catch(IOException ioe){
			ioe.printStackTrace();
		}
		return null;
	}
	
	/*
	应用过滤器来获取单条数据
	*/
	
	public static Result getRow(String tableName, String rowKey, FilterList filterList){
		try(Table table = HBase.getTable(tableName)){
			Get get = new Get(Bytes.toBytes(rowKey));
			get.setFilter(filterList);
			return table.get(get);
		}catch(IOException ioe){
			ioe.printStackTrace();
		}
		return null;
	}
	
	public static ResultScanner getScanner(String tableName){
		try(Table table = HBase.getTable(tableName)){
			Scan scan = new Scan();
			scan.setCaching(1000);
			return table.getScanner(scan);
			return table.get(get);
		}catch(IOException ioe){
			ioe.printStackTrace();
		}
		return null;
	}
	/*
	批量检索数据
	*/
	public static ResultScanner getScanner(String tableName, String startRowKey, String endRowKey){
	//包括start row key，但不包含end row key的
		try(Table table = HBase.getTable(tableName)){
			Scan scan = new Scan();
			scan.setStartRow(Bytes.toBytes(startRowKey));
			scan.setStopRow(Bytes.toBytes(endRowKey));
			scan.setCaching(1000);
			return table.getScanner(scan);
			return table.get(get);
		}catch(IOException ioe){
			ioe.printStackTrace();
		}
		return null;
	}
	
	/*
	使用过滤器进行批量检索数据
	*/
	public static ResultScanner getScanner(String tableName, String startRowKey, String endRowKey, FilterList filterList){
		try(Table table = HBase.getTable(tableName)){
			Scan scan = new Scan();
			scan.setStartRow(Bytes.toBytes(startRowKey));
			scan.setStopRow(Bytes.toBytes(endRowKey));
			scan.setFilter(filterList);
			scan.setCaching(1000);
			return table.getScanner(scan);
			return table.get(get);
		}catch(IOException ioe){
			ioe.printStackTrace();
		}
		return null;
	}
	
	/*
	删除1行数据
	*/
	public static boolean deleteRow(String nameTable, String rowKey){
		try(Table table = HBase.getTable(tableName)){
			Delete delete = new Delete(Bytes.toBytes(nameTable));
			table.delete(delete);
		}catch(IOException ioe){
			ioe.printStackTrace();
		}
		return true;
	}
	
	
	/*
	删除某个列族
	*/
	public static boolean deleteColumnFamily(String nameTable, String cfName){
		//因为是对表结构的修改，所以需要实例化一个Admin的实例
		try (HBaseAdmin admin = (HBaseAdmin)HBaseConn.getHBase.getHBaseConn().getAdmin()){
			admin.deleteColumn(nameTable, cfName);
		}catch(Exception e){
			e.printStackTrace();
		}
		return true;
	}
	
	public static boolean deleteQualifer(String nameTable, String rowKey, String cfName, String qualifier){
		//因为是对表结构的修改，所以需要实例化一个Admin的实例
		try(Table table = HBase.getTable(tableName)){
			Delete delete = new Delete(Bytes.toBytes(rowKey));
			delete.addColumn(Bytes.toBytes(cfName), Bytes.toBytes(qualifier));
			table.delete(delete);
		}catch(IOException ioe){
			ioe.printStackTrace();
		}
		return true;
	}
 }