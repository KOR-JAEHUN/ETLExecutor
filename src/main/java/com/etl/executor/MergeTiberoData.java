package com.etl.executor;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * LAKE -> DATA MART
 * @author song
 *
 */
public class MergeTiberoData {

//	private static final Logger logger = LoggerFactory.getLogger(Executor.class);
	
	Statement stmt = null;
	public void mergeTibero(String tableNm, Connection conn) throws Exception {
		try {
			IncrementInsertData iid = new IncrementInsertData();
			String sql = iid.sqlDeleteHub(tableNm, "TMP");
//			System.out.println(sql);
			stmt = conn.createStatement();
			int deleteCnt = stmt.executeUpdate(sql);
			System.out.println("삭제건수 = " + deleteCnt);
			
			stmt = conn.createStatement();
			sql = "insert into " + tableNm + "\r\n" + 
					"select * from " + tableNm + "_TMP";
//			sql = "insert into " + tableNm + "_BAK" + "\r\n" + 
//				  "select * from " + tableNm + "_TMP";
			stmt.execute(sql);
			stmt.close();
		} catch (SQLException e) {
			throw new Exception(e);
		} finally {
			try{
	        	if( stmt != null ){
	        		stmt.close();                
	        	}
	        }catch(Exception ex){
	        	stmt = null;
	        }
		}
	}
	
	public void deleteTibero(String tableNm, Connection conn) throws Exception {
		try {
			IncrementInsertData iid = new IncrementInsertData();
			String sql = iid.sqlDeleteHub(tableNm, "DEL");
			stmt = conn.createStatement();
			stmt.executeUpdate(sql);
			
			stmt.close();
		} catch (SQLException e) {
			throw new Exception(e);
		} finally {
			try{
				if( stmt != null ){
					stmt.close();                
				}
			}catch(Exception ex){
				stmt = null;
			}
		}
	}
//	
}
