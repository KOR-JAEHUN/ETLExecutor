package com.etl.executor;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReadHadoopData {

	private static final Logger logger = LoggerFactory.getLogger(Executor.class);
	final int MAX_INSERT_BATCH_COUNT = 200000; // 한번에 insert할 배치 카운트
	
	public List<HashMap<String,String>> readHadoopData(String tableNm, String dbNm, Connection conn, Connection tconn) throws Exception {
		ResultSet rs = null;
		List<HashMap<String,String>> list = new ArrayList<HashMap<String,String>>();
		try {
			
			// tibero에 테이블 삭제 후 추가
			CreateTiberoTable ct = new CreateTiberoTable();
			ct.copyTable(tableNm, tconn);
			
//			// hadoop
//			String sql = "select * from " + dbNm + "." + tableNm;
//			Statement stmt = conn.createStatement();
//			rs = stmt.executeQuery(sql);
//	        ResultSetMetaData rsmd = rs.getMetaData();
//	        List<String> columns = new ArrayList<String>(rsmd.getColumnCount());
//	        // 컬럼명 가져오기
//	        for(int i = 1; i <= rsmd.getColumnCount(); i++){
//	            columns.add(rsmd.getColumnName(i));
//	        }
//	        
//	        
//	        // tibero에 데이터 삽입
//	        int hCount = 1; // batch갯수
//	        while(rs.next()){
//	        	try {
//	        		HashMap<String,String> row = new LinkedHashMap<String, String>(columns.size());
//					 for(String col : columns) {
//					     row.put(col, rs.getString(col)); // 컬럼별 데이터
//					 }
//					 list.add(row);
//					 if(hCount%MAX_INSERT_BATCH_COUNT == 0) {
//						InsertTiberoData tb = new InsertTiberoData();
//			        	tb.insertTiberoData(list, tableNm, tconn);
//			        	System.out.println("count = " + hCount);
//			        	list = new ArrayList<HashMap<String,String>>();
//					 }
//				} catch (Exception e) {
//					logger.error(e.getMessage());
//					System.out.println("######### 적재 실패 ##########");
//					break;
//				} finally {
//					hCount++;
//				}
//	        }
//	        
//	        // 마지막 남은 데이터들 insert
//        	try {
//        		InsertTiberoData tb = new InsertTiberoData();
//				tb.insertTiberoData(list, tableNm, tconn);
//			} catch (Exception e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
//	        
//	        rs.close();
//	        conn.close();
		} catch (SQLException e) {
			throw new Exception(e);
		} finally {
			try{
	            if( rs != null ){
	                rs.close();                
	            }
	        }catch(Exception ex){
	            rs = null;
	        }
	        
//	        try{
//	            if( conn != null ){
//	                conn.close();                
//	            }
//	        }catch(Exception ex){
//	            conn = null;
//	        }
		}
		
		return list;
	}
}
