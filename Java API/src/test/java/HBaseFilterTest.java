 import org.junit.Test;
 import com.imooc.bigdata.hbase.api.HBaseUtil;
 
 public class HBaseFilterTest{
	
	@Test
	public void createTable(){
		HBaseUtil.createTable(tableName:"FileTable", new String[]{"fileInfo", "saveFileInfo"});
	}
	
	@Test
	public void addFileData(){
		HBaseUtil.putRow(tableName:"FileTable", rowKey:"rowkey1", cfName:"fileInfo", qualifier:"name", data:"file1.txt");
		HBaseUtil.putRow(tableName:"FileTable", rowKey:"rowkey1", cfName:"fileInfo", qualifier:"type", data:"txt");
		HBaseUtil.putRow(tableName:"FileTable", rowKey:"rowkey1", cfName:"fileInfo", qualifier:"size", data:"1024");
		HBaseUtil.putRow(tableName:"FileTable", rowKey:"rowkey1", cfName:"fileInfo", qualifier:"creator", data:"jasxie");
		
		HBaseUtil.putRow(tableName:"FileTable", rowKey:"rowkey2", cfName:"fileInfo", qualifier:"name", data:"file2.jpg");
		HBaseUtil.putRow(tableName:"FileTable", rowKey:"rowkey2", cfName:"fileInfo", qualifier:"type", data:"jpg");
		HBaseUtil.putRow(tableName:"FileTable", rowKey:"rowkey2", cfName:"fileInfo", qualifier:"size", data:"512");
		HBaseUtil.putRow(tableName:"FileTable", rowKey:"rowkey2", cfName:"fileInfo", qualifier:"creator", data:"zxie86");
		
		HBaseUtil.putRow(tableName:"FileTable", rowKey:"rowkey3", cfName:"fileInfo", qualifier:"name", data:"file3.pdf");
		HBaseUtil.putRow(tableName:"FileTable", rowKey:"rowkey3", cfName:"fileInfo", qualifier:"type", data:"pdf");
		HBaseUtil.putRow(tableName:"FileTable", rowKey:"rowkey3", cfName:"fileInfo", qualifier:"size", data:"2048");
		HBaseUtil.putRow(tableName:"FileTable", rowKey:"rowkey3", cfName:"fileInfo", qualifier:"creator", data:"zx2177");
	}
	
	@Test
	public void rowFilterTest(){
		String tableName = "FileTable";
		String rowKey = "rowkey1";
		String cfName = "fileInfo";
		String qualifier = "name";
		String startRowKey = "rowkey1";
		String endRowKey = "rowkey3";
		Filter filter = new rowFilter(CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes(rowKey)));
		
		FilterList filterList = new FilterList(Operator.MUST_PASS_ALL, Arrays.asList(filter));
		//MUST_PASS_ALL 当前filter 里面的全部的filter都必须通过才能算true
		ResultScanner scanner = HBaseUtil.getScanner(tableName, startRowKey, endRowKey, filterList);
			if(scanner != null){
			scanner.forEach(result->
				System.out.println("rowkey = " + Bytes.toString(result.getRow()));
				System.out.println("filename = " + Bytes.toString(result.getValue(Bytes.toBytes(cfName), Bytes.toBytes(qualifier))));
			);
		}
		scanner.close();
	}
	
	//prefix test
	@Test
	public void prefixFilterTest(){
		String tableName = "FileTable";
		String rowKey = "rowkey2";
		String cfName = "fileInfo";
		String qualifier = "name";
		String startRowKey = "rowkey1";
		String endRowKey = "rowkey3";
		Filter filter = new prefixFilter(Bytes.toBytes(rowKey));
		FilterList filterList = new FilterList(Operator.MUST_PASS_ALL, Arrays.asList(filter));
		ResultScanner scanner = HBaseUtil.getScanner(tableName, startRowKey, endRowKey, filterList);
			if(scanner != null){
			scanner.forEach(result->
				System.out.println("rowkey = " + Bytes.toString(result.getRow()));
				System.out.println("filename = " + Bytes.toString(result.getValue(Bytes.toBytes(cfName), Bytes.toBytes(qualifier))));
			);
		}
		scanner.close();
	}
	
	@Test
	public void keyOnlyFilterTest(){
		String tableName = "FileTable";
		String rowKey = "rowkey2";
		String cfName = "fileInfo";
		String qualifier = "name";
		String startRowKey = "rowkey1";
		String endRowKey = "rowkey3";
		Filter filter = new keyOnlyFilter(lenAsVal:true);//只返回ｒｏｗ　ｋｅｙ和ｃｏｌｕｍｎ的值而不会返回单元格的内容
		FilterList filterList = new FilterList(Operator.MUST_PASS_ALL, Arrays.asList(filter));
		ResultScanner scanner = HBaseUtil.getScanner(tableName, startRowKey, endRowKey, filterList);
			if(scanner != null){
			scanner.forEach(result->
				System.out.println("rowkey = " + Bytes.toString(result.getRow()));
				//这里指会有ｒｏｗｋｅｙ的值，而不会有ｆｉｌｅｎａｍｅ的值
				System.out.println("filename = " + Bytes.toString(result.getValue(Bytes.toBytes(cfName), Bytes.toBytes(qualifier))));
			);
		}
		scanner.close();
	}
	
	@Test
	public void columnPrefixFilterTest(){
		String tableName = "FileTable";
		String rowKey = "rowkey2";
		String cfName = "fileInfo";
		String qualifier = "name";
		String startRowKey = "rowkey1";
		String endRowKey = "rowkey3";
		Filter filter = new ColumnPrefixFilter(s:"nam");
		FilterList filterList = new FilterList(Operator.MUST_PASS_ALL, Arrays.asList(filter));
		ResultScanner scanner = HBaseUtil.getScanner(tableName, startRowKey, endRowKey, filterList);
			if(scanner != null){
			scanner.forEach(result->
				System.out.println("rowkey = " + Bytes.toString(result.getRow()));
				System.out.println("filename = " + Bytes.toString(result.getValue(Bytes.toBytes(cfName), Bytes.toBytes(qualifier))));
				System.out.println("fileType= " + Bytes.toString(result.getValue(Bytes.toBytes(cfName), Bytes.toBytes(s:"type"))));//fileType = null
			);
		}
		scanner.close();
	}
	
	@Test
	public static deleteTable(){
		//public static class boolean deleteTable(String tableName)
		HBaseUtil.deleteTable(tableName:"FileTable");
	}
}