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
	
	
	public void copyLake(String tableNm, String dbNm, Connection conn) throws Exception {
		Statement stmt1 = null;
		Statement stmt2 = null;
		try {
			String orgTableNm = tableNm;
			if("ods_plan".equals(dbNm) && "TCM_CMN_CD".equals(tableNm)) {
				tableNm = tableNm.replace("TCM_CMN_CD", "TCM_CMN_CD_PLAN");
			}
			String sql ="DROP TABLE IF EXISTS " +  lakeDbNm + "." + tableNm;
			System.out.println(sql);
			stmt1 = conn.createStatement();
			stmt1.execute(sql);
			stmt1.close();
			
			sql = "CREATE TABLE " + lakeDbNm + "." + tableNm + " AS SELECT * FROM " +  dbNm + "." + orgTableNm;
			System.out.println(sql);
			stmt2 = conn.createStatement();
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
}
