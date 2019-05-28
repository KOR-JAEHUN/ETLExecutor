package com.etl.executor;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * ODS -> LAKE로 넘어가는 HADOOP데이터 생성
 * @author song
 *
 */
public class CopyHadoopLake {
	
	private final String lakeDbNm = "big_unity"; // lake
	Statement stmt = null;
	
	public void copyLake(String tableNm, String dbNm, Connection conn) throws Exception {
		try {
			
			String sql ="DROP TABLE IF EXISTS " +  lakeDbNm + "." + tableNm;
			System.out.println(sql);
			stmt = conn.createStatement();
			stmt.execute(sql);
			
			sql = "CREATE TABLE " + lakeDbNm + "." + tableNm + " AS SELECT * FROM " +  dbNm + "." + tableNm;
			System.out.println(sql);
			stmt = conn.createStatement();
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
}
