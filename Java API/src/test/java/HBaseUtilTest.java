 import org.junit.Test;
 import com.imooc.bigdata.hbase.api.HBaseUtil;
 
 public class HBaseUtilTest{
	
	@Test
	public void createTable(){
		HBaseUtil.createTable(tableName:"FileTable", new String[]{"fileInfo", "saveFileInfo"});
	}
	
	@Test
	public void addFileData(){
		//putRow(String tableName, String rowKey, String cfName, String qualifier, String data)
		/*
		https://www.dummies.com/programming/big-data/hadoop/column-qualifiers-in-the-hbase-data-model/
	Row Key	Column Family: {Column Qualifier:Version:Value}
	00001	CustomerName: {‘FN’:
	1383859182496:‘John’,
	‘LN’: 1383859182858:‘Smith’,
	‘MN’: 1383859183001:’Timothy’,
	‘MN’: 1383859182915:’T’}
	ContactInfo: {‘EA’:
	1383859183030:‘John.Smith@xyz.com’,
	’SA’: 1383859183073:’1 Hadoop Lane, NY
	11111’}
		*/
		HBaseUtil.putRow(tableName:"FileTable", rowKey:"rowkey1", cfName:"fileInfo", qualifier:"name", data:"file1.txt");
		HBaseUtil.putRow(tableName:"FileTable", rowKey:"rowkey1", cfName:"fileInfo", qualifier:"type", data:"txt");
		HBaseUtil.putRow(tableName:"FileTable", rowKey:"rowkey1", cfName:"fileInfo", qualifier:"size", data:"1024");
		HBaseUtil.putRow(tableName:"FileTable", rowKey:"rowkey1", cfName:"fileInfo", qualifier:"creator", data:"jasxie");
		
		HBaseUtil.putRow(tableName:"FileTable", rowKey:"rowkey2", cfName:"fileInfo", qualifier:"name", data:"file2.jpg");
		HBaseUtil.putRow(tableName:"FileTable", rowKey:"rowkey2", cfName:"fileInfo", qualifier:"type", data:"jpg");
		HBaseUtil.putRow(tableName:"FileTable", rowKey:"rowkey2", cfName:"fileInfo", qualifier:"size", data:"512");
		HBaseUtil.putRow(tableName:"FileTable", rowKey:"rowkey2", cfName:"fileInfo", qualifier:"creator", data:"zxie86");
	}
	
	@Test
	public void getFileDetails(){
		String tableName = "FileTable";
		String rowKey = "rowkey1";
		String cfName = "fileInfo";
		String qualifier = "name";
		//Result result = HBaseUtil.getRow(tableName:"FileTable", rowKey:"rowkey1");
		Result result = HBaseUtil.getRow(tableName, rowKey);
		if(result != null){
			System.out.println("rowkey = " + Bytes.toString(result.getRow()));
			System.out.println("filename = " + Bytes.toString(result.getValue(Bytes.toBytes(cfName), Bytes.toBytes(qualifier))));
		}
	}
	
	@Test
	//检索数据
	public void scanFileDetails(){
	//public static ResultScanner getScanner(String tableName, String startRowKey, String endRowKey)
		String tableName = "FileTable";
		String startRowKey = "rowkey1";
		String endRowKey = "rowkey1";
		String cfName = "fileInfo";
		String qualifier = "name";
		ResultScanner scanner = HBaseUtil.getScanner(tableName, startRowKey, endRowKey);
		if(scanner != null){
			scanner.forEach(result->
				System.out.println("rowkey = " + Bytes.toString(result.getRow()));
				System.out.println("filename = " + Bytes.toString(result.getValue(Bytes.toBytes(cfName), Bytes.toBytes(qualifier))));
			);
		}
		scanner.close();//防止内存泄露
	}
	
	@Test
	//删除操作
	public static deleteRow(){
	//public static boolean deleteRow(String nameTable, String rowKey){
		String nameTable = "FileTable";
		String rowKey = "rowkey1";
		HBaseUtil.deleteRow(nameTable, rowKey);
	}
	
	@Test
	public static deleteTable(){
		//public static class boolean deleteTable(String tableName)
		HBaseUtil.deleteTable(tableName:"FileTable");
	}
 }