package com.etl.executor;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class MergeTiberoData {

//	private static final Logger logger = LoggerFactory.getLogger(Executor.class);
	
	public void mergeTibero(String tableNm, Connection conn) throws Exception {
		Statement stmt1 = null;
		Statement stmt2 = null;
		try {
			IncrementInsertData iid = new IncrementInsertData();
			String sql = iid.sqlDeleteHub(tableNm, "TMP");
//			System.out.println(sql);
			stmt1 = conn.createStatement();
			int deleteCnt = stmt1.executeUpdate(sql);
			System.out.println("TMP 삭제건수 = " + deleteCnt);
			stmt1.close();
			
			stmt2 = conn.createStatement();
			sql = "insert into " + tableNm + "\r\n" + 
					"select * from " + tableNm + "_TMP";
//			sql = "insert into " + tableNm + "_BAK" + "\r\n" + 
//				  "select * from " + tableNm + "_TMP";
			stmt2.execute(sql);
			stmt2.close();
		} catch (SQLException e) {
			throw new Exception(e);
		} finally {
			try{
	        	if( stmt1 != null ){
	        		stmt1.close();                
	        	}
	        }catch(Exception ex){
	        	stmt1 = null;
	        }
			try{
				if( stmt2 != null ){
					stmt2.close();                
				}
			}catch(Exception ex){
				stmt2 = null;
			}
		}
	}
	
	public void deleteTibero(String tableNm, Connection conn) throws Exception {
		Statement stmt = null;
		try {
			IncrementInsertData iid = new IncrementInsertData();
			String sql = iid.sqlDeleteHub(tableNm, "DEL");
			stmt = conn.createStatement();
			int cnt = stmt.executeUpdate(sql);
			System.out.println("DEL 삭제건수 = " + cnt);
			
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
