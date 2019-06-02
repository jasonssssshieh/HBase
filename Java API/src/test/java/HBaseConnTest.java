 import org.apache.hadoop.hbase.client.Connection;
 import org.junit.Test;
 import com.imooc.bigdata.hbase.api.HBaseConn;
 
 public HBaseConnTest{
	@Test
	public void getConnTest() {
		Configuration conf = HBaseConn.getHBaseConn();
		System.out.println(conn.isClosed());
		HBaseConn.closeConn();
		System.out.println(conn.isClosed());
	}
	
	@Table
	public void getTableTest(){
		try{
			Table table = HBaseConn.getTable(tableName:"US_POPULATION");
			System.out.println(table.getName().getNameAsString());
			table.close();
		}catch(IOException e){
			e.printStackTrace();
		}
	}
 }