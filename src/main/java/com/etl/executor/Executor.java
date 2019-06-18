package com.etl.executor;

import java.net.InetAddress;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class Executor {
	
	private static final Logger logger = LoggerFactory.getLogger(Executor.class);
	
	// 삭제를 위한 테이블
	private String[] delete_data_table = {"TAC_ACP_SBJT", "TEV_EVLR_SBJT_ESTM", "TSL_SLC_SBJT_RSLT", "TAC_KWD", "TCM_FILE_DTL", "TAC_SBJT_TPI_HR", "TAC_SMMR_CNTN", "TRT_PPR", "TEV_PN_EVLR_ESTM_SBJT",
										 "KCDM312", "KCDM310", "KCDD214", "NRF_BIG_SCJNL_INFO_ITG", "NRF_BIG_PPR_INFO_ITG", "NRF_BIG_PPR_ATHR_INFO_ITG"};
	
	public static void main(String[] args) {
        
        long startTime = System.currentTimeMillis();

        Executor exe = new Executor();
//        exe.copyTableOdsToLake();
        exe.etlStart();
//          exe.createMasterTable();
//        exe.startMongo();
//        exe.copyHadoopLog();
        
        long endTime = System.currentTimeMillis();
        long time = endTime - startTime;
        System.out.println("총 실행시간 =============== " + time/1000.0 + "초");
	}
	
	void etlStart() {
		InsertLog log = new InsertLog();
        ResultSet rs = null;
        Connection conn = null;
        Connection tconn = null;
        Statement stmt = null;
		try{
			InetAddress local = InetAddress.getLocalHost();
			String ip = local.getHostAddress();
//			System.out.println("server ip = " + ip);
	        System.out.println("######### 적재 시작 ##########");
	        tconn = DBConnByTibero.getInstacne().getConnection();
			conn = DBConnByHadoop.getInstacne().getConnection();
	        
			stmt = conn.createStatement();
	        
//	        String sql = "select * from ods_batch_hist.etl_table_list";
//	        String sql = "select * from ods_batch_hist.etl_table_list where table_nm in('TCM_CMN_CD') and type is null";
			String sql = "select * from ods_batch_hist.etl_table_list where table_nm in('TMN_SBJT_TPI_HR') and type is null";
//			String sql = "";
	        if("100.100.100.216".contains(ip)) {
	        	sql = "select * from ods_batch_hist.etl_table_list where db_nm = 'ods_ernd' or db_nm = 'ods_ofd100' and type is null";
	        	System.out.println("ERND,OFD100 TIBERO ETL START");
	        }else if("100.100.100.217".contains(ip)) {
	        	sql = "select * from ods_batch_hist.etl_table_list where db_nm = 'ods_kci' or db_nm = 'ods_plan' and type is null";
	        	System.out.println("KCI,PLAN TIBERO ETL START");
	        }else if("100.100.100.218".contains(ip)) {
		        sql = "select * from ods_batch_hist.etl_table_list where db_nm = 'ods_kri' or db_nm = 'ods_law' or db_nm = 'ods_sci' and type is null";
		        System.out.println("KRI,LAW,SCI TIBERO ETL START");
	        }
	        
	        rs = stmt.executeQuery(sql);
	        
	        String startDt = "";
	        String endDt = "";
	        String tableNm = "";
	        String dbNm = "";
	        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
	        ReadHadoopData hd = new ReadHadoopData();
	        while(rs.next()){
	        	try {
	        		long startTime = System.currentTimeMillis();
					startDt = sdf.format(new Date(startTime));
	        		
	        		tableNm = rs.getString("table_nm");
	        		dbNm = rs.getString("db_nm");
	        		if("ods_plan".equals(dbNm)) { // ernd와 plan db에 같은 테이블명이 존재하여 replace로 datamart쪽만 이름변경
						tableNm = tableNm.replace("TCM_CMN_CD", "TCM_CMN_CD_PLAN");
					}	        		
	        		System.out.println("적재 테이블 === " + dbNm + "." + tableNm);
	        		if(Arrays.asList(delete_data_table).contains(tableNm)) {
	        			hd.readAndDeleteHadoopData(tableNm, conn, tconn);
	        		}
	        		int cnt = hd.readAndInsertHadoopData(tableNm, dbNm, conn, tconn);
	        		
	        		long endTime = System.currentTimeMillis();
	        		endDt = sdf.format(new Date(endTime));
	        		long time = endTime - startTime;
	        		log.insertLog(dbNm.replace("ods", "datamart"), tableNm, conn, startDt, endDt, "SUCCESS", cnt);
	        		System.out.println(tableNm + " 테이블 실행시간 =============== " + time/1000.0 + "초");
				} catch (Exception e) {
					log.insertLog(dbNm.replace("ods", "datamart"), tableNm, conn, startDt, endDt, "FAIL", 0);
				}
	        }
	        
	        rs.close();
	        stmt.close();
	        conn.close();
	        tconn.close();
	        System.out.println("######### 적재 종료 ##########");
	        
	    }catch(Exception ex){
	    	logger.error(ex.getMessage());
			System.out.println("######### 적재 실패 ##########");
	        ex.printStackTrace();
	    }finally {
	        try{
	            if( rs != null ){
	                rs.close();                
	            }
	        }catch(Exception ex){
	            rs = null;
	        }
	        
	        try{
	        	if( stmt != null ){
	        		stmt.close();                
	        	}
	        }catch(Exception ex){
	        	stmt = null;
	        }
	        
	        try{
	            if( conn != null ){
	                conn.close();                
	            }
	        }catch(Exception ex){
	            conn = null;
	        }
	        
	        try{
	        	if( tconn != null ){
	        		tconn.close();                
	        	}
	        }catch(Exception ex){
	        	tconn = null;
	        }
	    }
	}

	// ODS에서 LAKE로 테이블 복제 
	public boolean copyTableOdsToLake() {
			
        ResultSet rs = null;
        Connection conn = null;
        Statement stmt = null;
		try{
			InetAddress local = InetAddress.getLocalHost();
			String ip = local.getHostAddress();
			
			System.out.println("######### ODS TO LAKE ##########");
	        System.out.println("######### 테이블 복제 시작 ##########");
			conn = DBConnByHadoop.getInstacne().getConnection();
	        
			stmt = conn.createStatement();
	        
	        String sql = "select * from ods_batch_hist.etl_table_list where table_nm in('TMN_SBJT_TPI_HR') and type is null";
//	        String sql = "";
	        if("100.100.100.216".contains(ip)) {
	        	sql = "select * from ods_batch_hist.etl_table_list where db_nm = 'ods_ernd' or db_nm = 'ods_ofd100' or db_nm = 'ods_jcr' and type is null";
	        	System.out.println("############# ERND LAKE COPY START #############");
	        }else if("100.100.100.217".contains(ip)) {
	        	sql = "select * from ods_batch_hist.etl_table_list where db_nm = 'ods_kci' or db_nm = 'ods_plan' or db_nm = 'ods_scopus' and type is null";
	        	System.out.println("############# KCI LAKE COPY START #############");
	        }else if("100.100.100.218".contains(ip)) {
		        sql = "select * from ods_batch_hist.etl_table_list where db_nm = 'ods_kri' or db_nm = 'ods_law' or db_nm = 'ods_sci' and type is null";
		        System.out.println("############# KRI LAKE COPY START #############");
	        }
	        
	        rs = stmt.executeQuery(sql);
	        
	        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
	        CopyHadoopLake lake = new CopyHadoopLake();
	        InsertLog log = new InsertLog();
			String startDt = "";
			String endDt = "";
			String tableNm = "";
	        while(rs.next()){
	        	try {
					long startTime = System.currentTimeMillis();
					startDt = sdf.format(new Date(startTime));

	        		tableNm = rs.getString("table_nm");
	        		String dbNm = rs.getString("db_nm");
	        		System.out.println("복제 테이블 === " + dbNm + "." + tableNm);
	        		lake.copyLake(tableNm, dbNm, conn);
	        		
	        		long endTime = System.currentTimeMillis();
	        		endDt = sdf.format(new Date(endTime));
	        		long time = endTime - startTime;
	        		System.out.println(tableNm + " Lake Copy 실행시간 =============== " + time/1000.0 + "초");
	        		log.insertLog("lake", tableNm, conn, startDt, endDt, "SUCCESS", 0);
				} catch (Exception e) {
					log.insertLog("lake", tableNm, conn, startDt, endDt, "FAIL", 0);
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
	        }
	        rs.close();
	        stmt.close();
	        conn.close();
	        System.out.println("######### 테이블 복제 성공 ##########");
	        
	    }catch(Exception ex){
	    	logger.error(ex.getMessage());
			System.out.println("######### 테이블 복제 실패 ##########");
	        ex.printStackTrace();
	        return false;
	    }finally {
	        try{
	            if( rs != null ){
	                rs.close();                
	            }
	        }catch(Exception ex){
	            rs = null;
	        }
	        
	        try{
	        	if( stmt != null ){
	        		stmt.close();                
	        	}
	        }catch(Exception ex){
	        	stmt = null;
	        }
	        
	        try{
	            if( conn != null ){
	                conn.close();                
	            }
	        }catch(Exception ex){
	            conn = null;
	        }
	        
	        
	    }
		return true;
	}
	
	public void createMasterTable() {
		
		Connection conn = null;
		Connection tconn = null;
		try{
			InetAddress local = InetAddress.getLocalHost();
			String ip = local.getHostAddress();
			
			System.out.println("######### CREATE MASTER ##########");
			System.out.println("######### 마스터 테이블 시작 ##########");
			conn = DBConnByHadoop.getInstacne().getConnection();
			tconn = DBConnByTibero.getInstacne().getConnection();
			
			String[] tableNmArr = null;
			if("100.100.100.216".contains(ip)) {
				tableNmArr = new String[] {"NRF_BIG_BOOK_INFO_ITG","NRF_BIG_ITL_PPR_RGT_INFO_ITG","NRF_BIG_PPR_INFO_ITG"};
			}else if("100.100.100.217".contains(ip)) {
				tableNmArr = new String[] {"NRF_BIG_RSCHR_INFO_ITG","NRF_BIG_SCJNL_INFO_ITG"};
			}else if("100.100.100.218".contains(ip)) {
				tableNmArr = new String[] {"NRF_BIG_AGC_INFO_ITG","NRF_BIG_PPR_ATHR_INFO_ITG"};
			}
			
			/*
			 * NRF_BIG_PPR_INFO_ITG(논문정보통합) - 논문
			 * NRF_BIG_ITL_PPR_RGT_INFO_ITG(지식재산권정보통합)
			 * NRF_BIG_AGC_INFO_ITG(기관정보통합)
			 * NRF_BIG_RSCHR_INFO_ITG(연구자정보통합)
	 		 * NRF_BIG_SCJNL_INFO_ITG(학술지정보통합) - 증분 필요
			 * NRF_BIG_BOOK_INFO_ITG(저역서정보통합)
			 * NRF_BIG_PPR_ATHR_INFO_ITG(논문저자) - 증분 필요
			 */
//			String[] tableNmArr = {"NRF_BIG_BOOK_INFO_ITG", "NRF_BIG_RSCHR_INFO_ITG", "NRF_BIG_SCJNL_INFO_ITG", "NRF_BIG_ITL_PPR_RGT_INFO_ITG" , "NRF_BIG_AGC_INFO_ITG", "NRF_BIG_PPR_INFO_ITG"
//								, "NRF_BIG_PPR_ATHR_INFO_ITG"};
//			tableNmArr = new String[] {"NRF_BIG_RSCHR_INFO_ITG", "NRF_BIG_BOOK_INFO_ITG"};
			
	        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
	        ReadHadoopData hd = new ReadHadoopData();
			InsertLog log = new InsertLog();
			String startDt = "";
			String endDt = "";
			for (String tableNm : tableNmArr) {
				try {
					long startTime = System.currentTimeMillis();
					startDt = sdf.format(new Date(startTime));
					CreateHadoopMaster cm = new CreateHadoopMaster();
					if("NRF_BIG_BOOK_INFO_ITG".equals(tableNm)) { // 저역서
						cm.createMaster_book(conn);
					}else if("NRF_BIG_RSCHR_INFO_ITG".equals(tableNm)) { // 연구자
						cm.createMaster_rschr(conn);
					}else if("NRF_BIG_ITL_PPR_RGT_INFO_ITG".equals(tableNm)){  // 지식재산권
						cm.createMaster_rgt(conn);
					}else if("NRF_BIG_PPR_INFO_ITG".equals(tableNm)) { // 논문(long)
						cm.createMaster_ppr(conn);
					}else if("NRF_BIG_PPR_ATHR_INFO_ITG".equals(tableNm)) { // 저자(long)
						cm.createMaster_athr(conn);
					}else if("NRF_BIG_SCJNL_INFO_ITG".equals(tableNm)) { // 학술지(long)
						cm.createMaster_scjnl(conn);
					}else if("NRF_BIG_AGC_INFO_ITG".equals(tableNm)) { // 기관
						cm.createMaster_agc(conn);
					}
					// 그냥 데이터만 넣는 경우 사용(모든 데이터)
//					int cnt = hd.readHadoopInsertTiberoMaster(tableNm, conn, tconn);
					// 증분을 사용할때 사용
					if(Arrays.asList(delete_data_table).contains(tableNm)) {
	        			hd.readAndDeleteHadoopData(tableNm, conn, tconn);
					}
					int cnt = hd.readAndInsertHadoopData(tableNm, "X", conn, tconn);
					long endTime = System.currentTimeMillis();
	        		endDt = sdf.format(new Date(endTime));
	        		long time = endTime - startTime;
	        		System.out.println(tableNm + " 마스터 테이블 실행시간 =============== " + time/1000.0 + "초");
					log.insertLog("datamart_master", tableNm, conn, startDt, endDt, "SUCCESS", cnt);
				} catch (Exception e) {
					long endTime = System.currentTimeMillis();
					endDt = sdf.format(new Date(endTime));
					System.out.println(e.getMessage());
					log.insertLog("datamart_master", tableNm, conn, startDt, endDt, "FAIL", 0);
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			conn.close();
			System.out.println("######### 마스터 테이블 성공 ##########");
			
		}catch(Exception ex){
			logger.error(ex.getMessage());
			System.out.println("######### 마스터 테이블 실패 ##########");
			ex.printStackTrace();
		}finally {
			try{
				if( conn != null ){
					conn.close();                
				}
			}catch(Exception ex){
				conn = null;
			}
			try{
				if( tconn != null ){
					tconn.close();                
				}
			}catch(Exception ex){
				tconn = null;
			}
		}
	}
	
	// 하둡 로그 테이블을 티베로 로그 테이블로 데이터 카피
	public void copyHadoopLog() {
		
		Connection conn = null;
		Connection tconn = null;
		Statement stmt = null;
		ResultSet rs = null;
		try{
			
			Calendar cal = Calendar.getInstance();
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
			String date = sdf.format(cal.getTime());
			
			System.out.println("######### INSERT LOG ##########");
			System.out.println("######### 로그 테이블 시작 ##########");
			conn = DBConnByHadoop.getInstacne().getConnection();
			tconn = DBConnByTibero.getInstacne().getConnection();
			
			stmt = conn.createStatement();
	        
	        String sql = "select batch_cd, sys_nm, tb_nm, pro_nm, dd, start_tm, end_tm, status, cnt from ods_batch_hist.ods_batch_stat_2\r\n" + 
	        		"where batch_cd = 'etl_tibero' and dd = '"+date+"'";
	        System.out.println(sql);
			rs = stmt.executeQuery(sql);
	        ResultSetMetaData rsmd = rs.getMetaData();
	        List<String> columns = new ArrayList<String>(rsmd.getColumnCount());
	        // 컬럼명 가져오기
	        for(int i = 1; i <= rsmd.getColumnCount(); i++){
	            columns.add(rsmd.getColumnName(i));
	        }
	        
	        List<HashMap<String,String>> list = new ArrayList<HashMap<String,String>>();
	        while(rs.next()){
	        	LinkedHashMap<String,String> row = new LinkedHashMap<String, String>(columns.size());
				for(String col : columns) {
					row.put(col, rs.getString(col)); // 컬럼별 데이터
				}
				list.add(row);
	        }
	        
	        InsertTiberoData tb = new InsertTiberoData();
	        tb.insertTiberoData(list, "tb_ods_batch_stat", tconn);
	        
			rs.close();
			stmt.close();
			conn.close();
			tconn.close();
			System.out.println("######### 로그 테이블 성공 ##########");
			
		}catch(Exception ex){
			logger.error(ex.getMessage());
			System.out.println("######### 로그 테이블 실패 ##########");
			ex.printStackTrace();
		}finally {
			try{
	            if( rs != null ){
	                rs.close();                
	            }
	        }catch(Exception ex){
	            rs = null;
	        }
	        
	        try{
	        	if( stmt != null ){
	        		stmt.close();                
	        	}
	        }catch(Exception ex){
	        	stmt = null;
	        }
	        
	        try{
	            if( conn != null ){
	                conn.close();                
	            }
	        }catch(Exception ex){
	            conn = null;
	        }
	        
	        try{
	        	if( tconn != null ){
	        		tconn.close();                
	        	}
	        }catch(Exception ex){
	        	tconn = null;
	        }
		}
	}
	
	final int MAX_INSERT_BATCH_COUNT_BY_MONGO = 50000; // 한번에 insert할 배치 카운트
	public void startMongo() {
		List<HashMap<String,String>> list = new ArrayList<HashMap<String,String>>();
		LinkedHashMap<String, String> hm = null;
		Connection tconn = null;
		MongoClient client = null;
		
		try{
			System.out.println("######### Mongo->Tibero 시작 ##########");
			tconn = DBConnByTibero.getInstacne().getConnection();
			
			/* 0. Get client */
			MongoCredential credential = MongoCredential.createCredential("nrfbigdata", "admin", "nrfbigdata2019".toCharArray());
			client = new MongoClient(Arrays.asList(
					   new ServerAddress("100.100.100.216", 27017),
					   new ServerAddress("100.100.100.217", 27017),
					   new ServerAddress("100.100.100.218", 27017)), credential, MongoClientOptions.builder().build());
			
			/* 1. Connect to DB */
			MongoDatabase database = client.getDatabase("nrfbigdata");
			
			/* 2. Get collection */
			MongoCollection<Document> collection = database.getCollection("nrf_keywords");
			
			/* 3. Create Query Object */
			long curDate = System.currentTimeMillis();
			SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
			String date = sdf.format(new Date(curDate));
			BasicDBObject field = new BasicDBObject();
			field.put("docdate", date);
			
	        /* 4. Get all documents */
//			FindIterable<Document> docs = collection.find(field).limit(100);
			FindIterable<Document> docs = collection.find();
			int i=1;
			String tableNm = "NRF_KEYWORDS";
			InsertTiberoData tb = new InsertTiberoData();
			
			/* 5. Insert Data to Tibero */
			for (Document doc : docs) {
//				System.out.println(doc.toJson());
				hm = new LinkedHashMap<>();
				Iterator<Entry<String, Object>> iter = doc.entrySet().iterator();
				while (iter.hasNext()) {
					Map.Entry<String, Object> entry = (Map.Entry<String, Object>) iter.next();
					if(!"_id".equals(entry.getKey())) {
						hm.put(entry.getKey(), String.valueOf(entry.getValue()));
					}
				}
				list.add(hm);
				
				 if(i%MAX_INSERT_BATCH_COUNT_BY_MONGO == 0) {
					tb.insertTiberoDataByNoCols(list, tableNm, tconn);
		        	list = new ArrayList<HashMap<String,String>>();
				 }
				 i++;
			}
			tb.insertTiberoDataByNoCols(list, tableNm, tconn);
			client.close();
			tconn.close();
		} catch(Exception ex){
			logger.error(ex.getMessage());
			System.out.println("######### Mongo->Tibero 실패 ##########");
			ex.printStackTrace();
		} finally {
			try{
				if( client != null ){
					client.close();                
				}
			}catch(Exception ex){
				client = null;
			}
	        try{
	        	if( tconn != null ){
	        		tconn.close();                
	        	}
	        }catch(Exception ex){
	        	tconn = null;
	        }
		}
			
	}


}
