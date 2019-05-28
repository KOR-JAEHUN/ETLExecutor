package com.etl.executor;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * LAKE -> DATA MART
 * @author song
 *
 */
public class InsertTiberoData {

//	private static final Logger logger = LoggerFactory.getLogger(Executor.class);
	final int MAX_INSERT_BATCH_COUNT = 25000; // 한번에 insert할 배치 카운트
	
	public int insertTiberoData(List<HashMap<String,String>> list, String tableNm, Connection conn) throws Exception {
		PreparedStatement ptst = null;
		int count = 1; // batch갯수
		try {
			for (Map<String, String> map : list) {
				StringBuffer cols = new StringBuffer();
				StringBuffer qmarks = new StringBuffer(); // 물음표 갯수
				
				// batch를 돌리기 위해선 하나의 ptst를 사용하기 위해 딱 한 번만 생성
				if(count == 1) {
					int i=1;
					for( Map.Entry<String, String> elem : map.entrySet() ){
						if(i!=1) {
							cols.append(",");
							qmarks.append(",");
						}
//						cols.append(elem.getKey().replace(".","_BAK.")); // 컬럼 바인딩
						cols.append(elem.getKey()); // 컬럼 바인딩
						qmarks.append("?"); 
						i++;
					}
					
//					StringBuffer sql = new StringBuffer(" INSERT INTO " + tableNm + "_BAK" + " (" + cols + ") " + " VALUES ( " + qmarks + " ) ");
					StringBuffer sql = new StringBuffer(" INSERT INTO " + tableNm + " (" + cols + ") " + " VALUES ( " + qmarks + " ) ");
					ptst = conn.prepareStatement(sql.toString());
				}
				
				// 데이터 값 바인딩
				int j=1;
				for( Map.Entry<String, String> elem : map.entrySet() ){
					ptst.setString(j, elem.getValue());
					j++;
				}
				
				ptst.addBatch();
				ptst.clearParameters();
				
				if(count%MAX_INSERT_BATCH_COUNT == 0) {
					ptst.executeBatch();
					ptst.clearBatch();
					conn.commit();
				}
				
	            count++;
			}
			ptst.executeBatch(); // 남은 데이터 insert
			ptst.clearBatch();
			conn.commit();
		} catch (Exception e) {
			throw new Exception(e.getMessage());
		} finally {
	        try{
	        	if( ptst != null ){
	        		ptst.close();                
	        	}
	        }catch(Exception ex){
	        	ptst = null;
	        }
		}
		
		return count;
	}
	
	public int insertTiberoTmpData(List<HashMap<String,String>> list, String tableNm, Connection conn) throws Exception {
		PreparedStatement ptst = null;
		int count = 1; // batch갯수
		try {
			for (Map<String, String> map : list) {
				StringBuffer cols = new StringBuffer();
				StringBuffer qmarks = new StringBuffer(); // 물음표 갯수
				
				// batch를 돌리기 위해선 하나의 ptst를 사용하기 위해 딱 한 번만 생성
				if(count == 1) {
					int i=1;
					for( Map.Entry<String, String> elem : map.entrySet() ){
						if(i!=1) {
							cols.append(",");
							qmarks.append(",");
						}
						cols.append(elem.getKey().replace(".","_TMP.")); // 컬럼 바인딩
						qmarks.append("?"); 
						i++;
					}
					
					StringBuffer sql = new StringBuffer(" INSERT INTO " + tableNm + "_TMP" + " (" + cols + ") " + " VALUES ( " + qmarks + " ) ");
					ptst = conn.prepareStatement(sql.toString());
				}
				
				// 데이터 값 바인딩
				int j=1;
				for( Map.Entry<String, String> elem : map.entrySet() ){
					ptst.setString(j, elem.getValue());
					j++;
				}
				
				ptst.addBatch();
				ptst.clearParameters();
				
				if(count%MAX_INSERT_BATCH_COUNT == 0) {
					ptst.executeBatch();
					ptst.clearBatch();
					conn.commit();
				}
				
				count++;
			}
			ptst.executeBatch(); // 남은 데이터 insert
			ptst.clearBatch();
			conn.commit();
		} catch (Exception e) {
			throw new Exception(e.getMessage());
		} finally {
			try{
				if( ptst != null ){
					ptst.close();                
				}
			}catch(Exception ex){
				ptst = null;
			}
		}
		
		return count;
	}
	
	public int insertTiberoDelData(List<HashMap<String,String>> list, String tableNm, Connection conn) throws Exception {
		PreparedStatement ptst = null;
		int count = 1; // batch갯수
		try {
			for (Map<String, String> map : list) {
				StringBuffer cols = new StringBuffer();
				StringBuffer qmarks = new StringBuffer(); // 물음표 갯수
				
				// batch를 돌리기 위해선 하나의 ptst를 사용하기 위해 딱 한 번만 생성
				if(count == 1) {
					int i=1;
					for( Map.Entry<String, String> elem : map.entrySet() ){
						if(i!=1) {
							cols.append(",");
							qmarks.append(",");
						}
						cols.append(elem.getKey().replace(".","_DEL.")); // 컬럼 바인딩
						qmarks.append("?"); 
						i++;
					}
					
					StringBuffer sql = new StringBuffer(" INSERT INTO " + tableNm + "_DEL" + " (" + cols + ") " + " VALUES ( " + qmarks + " ) ");
					ptst = conn.prepareStatement(sql.toString());
				}
				
				// 데이터 값 바인딩
				int j=1;
				for( Map.Entry<String, String> elem : map.entrySet() ){
					ptst.setString(j, elem.getValue());
					j++;
				}
				
				ptst.addBatch();
				ptst.clearParameters();
				
				if(count%MAX_INSERT_BATCH_COUNT == 0) {
					ptst.executeBatch();
					ptst.clearBatch();
					conn.commit();
				}
				
				count++;
			}
			ptst.executeBatch(); // 남은 데이터 insert
			ptst.clearBatch();
			conn.commit();
		} catch (Exception e) {
			throw new Exception(e.getMessage());
		} finally {
			try{
				if( ptst != null ){
					ptst.close();                
				}
			}catch(Exception ex){
				ptst = null;
			}
		}
		
		return count;
	}
}
