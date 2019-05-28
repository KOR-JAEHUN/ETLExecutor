package com.etl.executor;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class InsertLog {
	Statement stmt = null;
	
	public void insertLog(String dbNm, String tableNm, Connection conn, String startDt, String endDt, String status, int cnt) throws Exception {
		try {
			String date = startDt.split(" ")[0];
			String sql = "insert into ods_batch_hist.ods_batch_stat_2 \r\n" + 
					     "values ('etl_tibero', '"+dbNm+"', '"+tableNm+"', 'etl', '"+date+"', '"+startDt.split(" ")[1]+"', '"+endDt.split(" ")[1]+"', '"+status+"', '"+cnt+"')";
			System.out.println(" LOG == " + sql);
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
}
