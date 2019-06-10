package com.etl.executor;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReadHadoopData {

	private static final Logger logger = LoggerFactory.getLogger(Executor.class);
	final int MAX_INSERT_BATCH_COUNT = 150000; // 한번에 insert할 배치 카운트
	private final String lakeDbNm = "big_unity"; // lake
	
	// 증분을 위한 테이블
	private String[] insert_data_table = {"NKRDD305", "NKRDD306", "NKRDD212", "NKRDD312", "NKRDD206", "NKRDD205", "NKRDD215", "NKRDD204", "NKRDD208", "NKRDM201",
										 "NKRDD315", "NKRDD311", "NKRDD310", "NKRDD210", "NKRDD309", "NKRDD209", "NKRDD303", "NKRDD203", "NKRDM270", "NKRDM279", "NKRDD202", "NKRDD302", "NKRDD304",
										 "NKRDD308", "TAC_ACP_SBJT", "TEV_EVLR_SBJT_ESTM", "TSL_SLC_SBJT_RSLT", "TAC_KWD", "TCM_FILE_DTL", "TAC_SBJT_TPI_HR", "TAC_SMMR_CNTN", "TRT_PPR",
										 "TEV_PN_EVLR_ESTM_SBJT", "KCDM312", "KCDM310", "KCDD214", "NRF_BIG_SCJNL_INFO_ITG", "NRF_BIG_PPR_INFO_ITG", "NRF_BIG_PPR_ATHR_INFO_ITG"};
	
	public int readAndInsertHadoopData(String tableNm, String dbNm, Connection conn, Connection tconn) throws Exception {
		ResultSet rs = null;
		List<HashMap<String,String>> list = new ArrayList<HashMap<String,String>>();
		int hCount = 1; // batch갯수
		try {
			
			/**/
			// hadoop
			// 해당 배열에 있는 테이블이면 증분만 가져오는 sql생성(삭제도 같이)
			String sql = null;
			boolean mergeFlag = false;
			if(Arrays.asList(insert_data_table).contains(tableNm)) {
				IncrementInsertData iid = new IncrementInsertData();
				sql = iid.sqlSelectHub(tableNm, "TMP");
				CreateTiberoTable ct = new CreateTiberoTable();
				ct.copyTmpTable(tableNm, tconn); // 임시 테이블 생성
				mergeFlag = true;
			}else {
				sql = "select * from " + lakeDbNm + "." + tableNm;
				// tibero에 테이블 삭제 후 추가
				CreateTiberoTable ct = new CreateTiberoTable();
				ct.copyTable(tableNm, tconn);
			}
			System.out.println(sql);
			
			Statement stmt = conn.createStatement();
			rs = stmt.executeQuery(sql);
			
	        ResultSetMetaData rsmd = rs.getMetaData();
	        List<String> columns = new ArrayList<String>(rsmd.getColumnCount());
	        // 컬럼명 가져오기
	        for(int i = 1; i <= rsmd.getColumnCount(); i++){
	            columns.add(rsmd.getColumnName(i));
	        }
	        
	        
	        // tibero에 데이터 삽입
	        System.out.println(tableNm + " 테이블 데이터 삽입 시작 ");
	        InsertTiberoData tb = new InsertTiberoData();
	        while(rs.next()){
	        	try {
	        		HashMap<String,String> row = new LinkedHashMap<String, String>(columns.size());
					 for(String col : columns) {
					     row.put(col, rs.getString(col)); // 컬럼별 데이터
					 }
					 list.add(row);
					 if(hCount%MAX_INSERT_BATCH_COUNT == 0) {
						 
						// tmp테이블을 삽입
						 
						// 증분시 수정값들은 키값이 같으므로 동일키값에 대한 데이터 삭제
						if(mergeFlag) {
							System.out.println("임시 테이블 데이터 삽입중");
							tb.insertTiberoTmpData(list, tableNm, tconn);
						}else {
							tb.insertTiberoData(list, tableNm, tconn);
						}
//			        	System.out.println(tableNm + " 테이블 count = " + hCount + " 삽입중");
			        	list = new ArrayList<HashMap<String,String>>();
					 }
				} catch (Exception e) {
					logger.error(e.getMessage());
					System.out.println("######### 적재 실패 ##########");
					break;
				} finally {
					hCount++;
				}
	        }
	        // 마지막 남은 데이터들 insert
	        if(list.size() > 0 && list != null) {
				if(mergeFlag) {
					System.out.println("임시 테이블 데이터 삽입중");
					tb.insertTiberoTmpData(list, tableNm, tconn); // 임시 테이블에 새로운 데이터 삽입
					MergeTiberoData db = new MergeTiberoData();
					db.mergeTibero(tableNm, tconn); // 임시 테이블로 기존 테이블에 있는 데이터 삭제 후 삽입
				}else {
					tb.insertTiberoData(list, tableNm, tconn);
				}
	        }
				
        	System.out.println(tableNm + " 데이터 총  " + (--hCount) + " 건");
	        rs.close();
			 
		} catch (SQLException e) {
			System.out.println(e.getMessage());
			throw new Exception(e);
		} finally {
			try{
	            if( rs != null ){
	                rs.close();                
	            }
	        }catch(Exception ex){
	            rs = null;
	        }
	        
		}
		
		return hCount;
	}
	
	// 삭제데이터 전용 메소드
	public List<HashMap<String,String>> readAndDeleteHadoopData(String tableNm, Connection conn, Connection tconn) throws Exception {
		ResultSet rs = null;
		List<HashMap<String,String>> list = new ArrayList<HashMap<String,String>>();
		try {
			
			String sql = null;
				
			CreateTiberoTable ct = new CreateTiberoTable();
			ct.copyDelTable(tableNm, tconn);
			
			IncrementInsertData iid = new IncrementInsertData();
			sql = iid.sqlSelectHub(tableNm, "DEL");
			
			Statement stmt = conn.createStatement();
			rs = stmt.executeQuery(sql);
			
			ResultSetMetaData rsmd = rs.getMetaData();
			List<String> columns = new ArrayList<String>(rsmd.getColumnCount());
			// 컬럼명 가져오기
			for(int i = 1; i <= rsmd.getColumnCount(); i++){
				columns.add(rsmd.getColumnName(i));
			}
			
			// tibero에 데이터 삽입
			int hCount = 1; // batch갯수
			InsertTiberoData tb = new InsertTiberoData();
			while(rs.next()){
				try {
					HashMap<String,String> row = new LinkedHashMap<String, String>(columns.size());
					for(String col : columns) {
						row.put(col, rs.getString(col)); // 컬럼별 데이터
					}
					list.add(row);
					if(hCount%MAX_INSERT_BATCH_COUNT == 0) {
						tb.insertTiberoDelData(list, tableNm, tconn);
						list = new ArrayList<HashMap<String,String>>();
					}
				} catch (Exception e) {
					logger.error(e.getMessage());
					System.out.println("######### 적재 실패 ##########");
					break;
				} finally {
					hCount++;
				}
			}
			// 마지막 남은 데이터들 insert
			if(list.size() > 0 && list != null) {
				tb.insertTiberoDelData(list, tableNm, tconn);
				 MergeTiberoData db = new MergeTiberoData();
				 db.deleteTibero(tableNm, tconn); // 삭제 테이블로 기존 테이블에 있는 데이터 삭제
			}
			
			rs.close();
			
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
			
		}
		
		return list;
	}
	
	// 하둡 마스터 데이터를 티베로 마스터 테이블로 이동
	public int readHadoopInsertTiberoMaster(String tableNm, Connection conn, Connection tconn) throws Exception {
		ResultSet rs = null;
		List<HashMap<String,String>> list = new ArrayList<HashMap<String,String>>();
		int hCount = 1; // batch갯수
		try {
			
			/**/
			// hadoop
			// 해당 배열에 있는 테이블이면 증분만 가져오는 sql생성(삭제도 같이)
			String sql = "select * from " + lakeDbNm + "." + tableNm;
			// tibero에 테이블 삭제 후 추가
			CreateTiberoTable ct = new CreateTiberoTable();
			ct.copyTable(tableNm, tconn);
			System.out.println(sql);
			
			Statement stmt = conn.createStatement();
			rs = stmt.executeQuery(sql);
			
	        ResultSetMetaData rsmd = rs.getMetaData();
	        List<String> columns = new ArrayList<String>(rsmd.getColumnCount());
	        // 컬럼명 가져오기
	        for(int i = 1; i <= rsmd.getColumnCount(); i++){
	            columns.add(rsmd.getColumnName(i));
	        }
	        
	        
	        // tibero에 데이터 삽입
	        System.out.println(tableNm + " 테이블 데이터 삽입 시작 ");
	        while(rs.next()){
	        	try {
	        		HashMap<String,String> row = new LinkedHashMap<String, String>(columns.size());
					 for(String col : columns) {
					     row.put(col, rs.getString(col)); // 컬럼별 데이터
					 }
					 list.add(row);
					 if(hCount%MAX_INSERT_BATCH_COUNT == 0) {
						 
						// tmp테이블을 삽입
						 
						// 증분시 수정값들은 키값이 같으므로 동일키값에 대한 데이터 삭제
						InsertTiberoData tb = new InsertTiberoData();
						tb.insertTiberoData(list, tableNm, tconn);
//			        	System.out.println(tableNm + " 테이블 count = " + hCount + " 삽입중");
			        	list = new ArrayList<HashMap<String,String>>();
					 }
				} catch (Exception e) {
					logger.error(e.getMessage());
					System.out.println("######### 적재 실패 ##########");
					break;
				} finally {
					hCount++;
				}
	        }
	        // 마지막 남은 데이터들 insert
	        if(list.size() > 0 && list != null) {
				InsertTiberoData tb = new InsertTiberoData();
				tb.insertTiberoData(list, tableNm, tconn);
	        }
				
        	System.out.println(tableNm + " 데이터 총  " + (--hCount) + " 건");
	        rs.close();
			 
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
	        
		}
		
		return hCount;
	}
}
