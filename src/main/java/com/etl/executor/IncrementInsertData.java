package com.etl.executor;

import java.text.SimpleDateFormat;
import java.util.Calendar;

/**
 * 증분에 대한 클래스
 * 수정분은 삭제 후 삽입해야하기때문에 이 클래스에서는 수정분에 대한 삭제와 증분에 대한 sql을 만들어 return해준다
 * @author song
 *
 */
public class IncrementInsertData {
	
	public String sqlSelectHub(String tableNm, String type) {
		String sql = null;
		Calendar cal = Calendar.getInstance();
		cal.add(Calendar.DATE, -1);
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		String date = sdf.format(cal.getTime());
//		date = "2019-05-30";
		if("NKRDD305".equals(tableNm) || "NKRDD306".equals(tableNm)  || "NKRDD312".equals(tableNm) || "NKRDD212".equals(tableNm) || "NKRDD206".equals(tableNm) || "NKRDD205".equals(tableNm)
				|| "NKRDD215".equals(tableNm) || "NKRDD204".equals(tableNm) || "NKRDD208".equals(tableNm) || "NKRDM201".equals(tableNm) || "NKRDD315".equals(tableNm)
				|| "NKRDD311".equals(tableNm) || "NKRDD310".equals(tableNm) || "NKRDD210".equals(tableNm) || "NKRDD309".equals(tableNm) || "NKRDD209".equals(tableNm)
				|| "NKRDD303".equals(tableNm) || "NKRDD203".equals(tableNm) || "NKRDD202".equals(tableNm) || "NKRDD302".equals(tableNm) || "NKRDD304".equals(tableNm)
				|| "NKRDD308".equals(tableNm)) {
			sql = getIncrement_NKRDDList(date, tableNm);
//			System.out.println(sql);
		}else if("NKRDM270".equals(tableNm) || "NKRDM279".equals(tableNm) ) {
			sql = getIncrement_NKRDMList(date, tableNm);
		}else if("TAC_ACP_SBJT".equals(tableNm) && "DEL".equals(type)) {
			sql = getIncrement_TAC_ACP_SBJT_DELList(date, tableNm);
		}else if("TAC_ACP_SBJT".equals(tableNm) && "TMP".equals(type)) {
			sql = getIncrement_ERNDTMPList(date, tableNm);
		}else if("TEV_EVLR_SBJT_ESTM".equals(tableNm) && "DEL".equals(type)) {
			sql = getIncrement_TEV_EVLR_SBJT_ESTM_DELList(date, tableNm);
		}else if("TEV_EVLR_SBJT_ESTM".equals(tableNm) && "TMP".equals(type)) {
			sql = getIncrement_ERNDTMPList(date, tableNm);
		}else if("TSL_SLC_SBJT_RSLT".equals(tableNm) && "DEL".equals(type)) {
			sql = getIncrement_TSL_SLC_SBJT_RSLT_DELList(date, tableNm);
		}else if("TSL_SLC_SBJT_RSLT".equals(tableNm) && "TMP".equals(type)) {
			sql = getIncrement_ERNDTMPList(date, tableNm);
		}else if("TAC_KWD".equals(tableNm) && "DEL".equals(type)) {
			sql = getIncrement_TAC_KWD_DELList(date, tableNm);
		}else if("TAC_KWD".equals(tableNm) && "TMP".equals(type)) {
			sql = getIncrement_ERNDTMPList(date, tableNm);
		}else if("TCM_FILE_DTL".equals(tableNm) && "DEL".equals(type)) {
			sql = getIncrement_TCM_FILE_DTL_DELList(date, tableNm);
		}else if("TCM_FILE_DTL".equals(tableNm) && "TMP".equals(type)) {
			sql = getIncrement_ERNDTMPList(date, tableNm);
		}else if("TAC_SBJT_TPI_HR".equals(tableNm) && "DEL".equals(type)) {
			sql = getIncrement_TAC_SBJT_TPI_HR_DELList(date, tableNm);
		}else if("TAC_SBJT_TPI_HR".equals(tableNm) && "TMP".equals(type)) {
			sql = getIncrement_ERNDTMPList(date, tableNm);
		}else if("TAC_SMMR_CNTN".equals(tableNm) && "DEL".equals(type)) {
			sql = getIncrement_TAC_SMMR_CNTN_DELList(date, tableNm);
		}else if("TAC_SMMR_CNTN".equals(tableNm) && "TMP".equals(type)) {
			sql = getIncrement_ERNDTMPList(date, tableNm);
		}else if("TRT_PPR".equals(tableNm) && "DEL".equals(type)) {
			sql = getIncrement_TRT_PPR_DELList(date, tableNm);
		}else if("TRT_PPR".equals(tableNm) && "TMP".equals(type)) {
			sql = getIncrement_ERNDTMPList(date, tableNm);
		}else if("TEV_PN_EVLR_ESTM_SBJT".equals(tableNm) && "DEL".equals(type)) {
			sql = getIncrement_TEV_PN_EVLR_ESTM_SBJT_DELList(date, tableNm);
		}else if("TEV_PN_EVLR_ESTM_SBJT".equals(tableNm) && "TMP".equals(type)) {
			sql = getIncrement_ERNDTMPList(date, tableNm);
		}else if("KCDM312".equals(tableNm) && "DEL".equals(type)) {
			sql = getIncrement_KCDM312_DELList(date, tableNm);
		}else if("KCDM312".equals(tableNm) && "TMP".equals(type)) {
			date = date.replaceAll("-", "");
			sql = getIncrement_KCDM312TMPList(date, tableNm);
		}else if("KCDM310".equals(tableNm) && "DEL".equals(type)) {
			sql = getIncrement_KCDM310_DELList(date, tableNm);
		}else if("KCDM310".equals(tableNm) && "TMP".equals(type)) {
			date = date.replaceAll("-", "");
			sql = getIncrement_KCDM310TMPList(date, tableNm);
		}else if("KCDD214".equals(tableNm) && "DEL".equals(type)) {
			sql = getIncrement_KCDD214_DELList(date, tableNm);
		}else if("KCDD214".equals(tableNm) && "TMP".equals(type)) {
			sql = getIncrement_KCDD214TMPList(date, tableNm);
		}else if("NRF_BIG_SCJNL_INFO_ITG".equals(tableNm) && "DEL".equals(type)) {
			sql = getIncrement_NRF_BIG_SCJNL_INFO_ITG_DELList(date, tableNm);
		}else if("NRF_BIG_SCJNL_INFO_ITG".equals(tableNm) && "TMP".equals(type)) {
			sql = getIncrement_NRF_BIG_SCJNL_INFO_ITGTMPList(date, tableNm);
		}else if("NRF_BIG_PPR_INFO_ITG".equals(tableNm) && "DEL".equals(type)) {
			sql = getIncrement_NRF_BIG_PPR_INFO_ITG_DELList(date, tableNm);
		}else if("NRF_BIG_PPR_INFO_ITG".equals(tableNm) && "TMP".equals(type)) {
			sql = getIncrement_NRF_BIG_PPR_INFO_ITGTMPList(date, tableNm);
		}else if("NRF_BIG_PPR_ATHR_INFO_ITG".equals(tableNm) && "DEL".equals(type)) {
			sql = getIncrement_NRF_BIG_PPR_ATHR_INFO_ITG_DELList(date, tableNm);
		}else if("NRF_BIG_PPR_ATHR_INFO_ITG".equals(tableNm) && "TMP".equals(type)) {
			sql = getIncrement_NRF_BIG_PPR_ATHR_INFO_ITGTMPList(date, tableNm);
		}
//		System.out.println(sql);
		return sql;
	}
	
	// update분에 대한 기존 데이터 삭제
	public String sqlDeleteHub(String tableNm, String type) {
		String sql = null;
		
		if("NKRDD305".equals(tableNm) && "TMP".equals(type)) {
			sql = deleteIncrement_NKRDD305List();
		}else if("NKRDD306".equals(tableNm) && "TMP".equals(type)) {
			sql = deleteIncrement_NKRDD306List();
		}else if("NKRDD312".equals(tableNm) && "TMP".equals(type)) {
			sql = deleteIncrement_NKRDD312List();
		}else if("NKRDD212".equals(tableNm) && "TMP".equals(type)) {
			sql = deleteIncrement_NKRDD212List();
		}else if("NKRDD206".equals(tableNm) && "TMP".equals(type)) {
			sql = deleteIncrement_NKRDD206List();
		}else if("NKRDD205".equals(tableNm) && "TMP".equals(type)) {
			sql = deleteIncrement_NKRDD205List();
		}else if("NKRDD215".equals(tableNm) && "TMP".equals(type)) {
			sql = deleteIncrement_NKRDD215List();
		}else if("NKRDD204".equals(tableNm) && "TMP".equals(type)) {
			sql = deleteIncrement_NKRDD204List();
		}else if("NKRDD208".equals(tableNm) && "TMP".equals(type)) {
			sql = deleteIncrement_NKRDD208List();
		}else if("NKRDM201".equals(tableNm) && "TMP".equals(type)) {
			sql = deleteIncrement_NKRDM201List();
		}else if("NKRDD315".equals(tableNm) && "TMP".equals(type)) {
			sql = deleteIncrement_NKRDD315List();
		}else if("NKRDD311".equals(tableNm) && "TMP".equals(type)) {
			sql = deleteIncrement_NKRDD311List();
		}else if("NKRDD310".equals(tableNm) && "TMP".equals(type)) {
			sql = deleteIncrement_NKRDD310List();
		}else if("NKRDD210".equals(tableNm) && "TMP".equals(type)) {
			sql = deleteIncrement_NKRDD210List();
		}else if("NKRDD309".equals(tableNm) && "TMP".equals(type)) {
			sql = deleteIncrement_NKRDD309List();
		}else if("NKRDD209".equals(tableNm) && "TMP".equals(type)) {
			sql = deleteIncrement_NKRDD209List();
		}else if("NKRDD303".equals(tableNm) && "TMP".equals(type)) {
			sql = deleteIncrement_NKRDD303List();
		}else if("NKRDD203".equals(tableNm) && "TMP".equals(type)) {
			sql = deleteIncrement_NKRDD203List();
		}else if("NKRDM270".equals(tableNm) && "TMP".equals(type)) {
			sql = deleteIncrement_NKRDM270List();
		}else if("NKRDM279".equals(tableNm) && "TMP".equals(type)) {
			sql = deleteIncrement_NKRDM279List();
		}else if("NKRDD202".equals(tableNm) && "TMP".equals(type)) {
			sql = deleteIncrement_NKRDD202List();
		}else if("NKRDD302".equals(tableNm) && "TMP".equals(type)) {
			sql = deleteIncrement_NKRDD302List();
		}else if("NKRDD304".equals(tableNm) && "TMP".equals(type)) {
			sql = deleteIncrement_NKRDD304List();
		}else if("NKRDD308".equals(tableNm) && "TMP".equals(type)) {
			sql = deleteIncrement_NKRDD308List();
		}else if("TAC_ACP_SBJT".equals(tableNm) && "DEL".equals(type)) {
			sql = deleteIncrement_TAC_ACP_SBJTDelList();
		}else if("TAC_ACP_SBJT".equals(tableNm) && "TMP".equals(type)) {
			sql = deleteIncrement_TAC_ACP_SBJTTmpList();
		}else if("TEV_EVLR_SBJT_ESTM".equals(tableNm) && "DEL".equals(type)) {
			sql = deleteIncrement_TEV_EVLR_SBJT_ESTMDelList();
		}else if("TEV_EVLR_SBJT_ESTM".equals(tableNm) && "TMP".equals(type)) {
			sql = deleteIncrement_TEV_EVLR_SBJT_ESTMTmpList();
		}else if("TSL_SLC_SBJT_RSLT".equals(tableNm) && "DEL".equals(type)) {
			sql = deleteIncrement_TSL_SLC_SBJT_RSLTDelList();
		}else if("TSL_SLC_SBJT_RSLT".equals(tableNm) && "TMP".equals(type)) {
			sql = deleteIncrement_TSL_SLC_SBJT_RSLTTmpList();
		}else if("TAC_KWD".equals(tableNm) && "DEL".equals(type)) {
			sql = deleteIncrement_TAC_KWDDelList();
		}else if("TAC_KWD".equals(tableNm) && "TMP".equals(type)) {
			sql = deleteIncrement_TAC_KWDTmpList();
		}else if("TCM_FILE_DTL".equals(tableNm) && "DEL".equals(type)) {
			sql = deleteIncrement_TCM_FILE_DTLDelList();
		}else if("TCM_FILE_DTL".equals(tableNm) && "TMP".equals(type)) {
			sql = deleteIncrement_TCM_FILE_DTLTmpList();
		}else if("TAC_SBJT_TPI_HR".equals(tableNm) && "DEL".equals(type)) {
			sql = deleteIncrement_TAC_SBJT_TPI_HRDelList();
		}else if("TAC_SBJT_TPI_HR".equals(tableNm) && "TMP".equals(type)) {
			sql = deleteIncrement_TAC_SBJT_TPI_HRTmpList();
		}else if("TAC_SMMR_CNTN".equals(tableNm) && "DEL".equals(type)) {
			sql = deleteIncrement_TAC_SMMR_CNTNDelList();
		}else if("TAC_SMMR_CNTN".equals(tableNm) && "TMP".equals(type)) {
			sql = deleteIncrement_TAC_SMMR_CNTNTmpList();
		}else if("TRT_PPR".equals(tableNm) && "DEL".equals(type)) {
			sql = deleteIncrement_TRT_PPRDelList();
		}else if("TRT_PPR".equals(tableNm) && "TMP".equals(type)) {
			sql = deleteIncrement_TRT_PPRTmpList();
		}else if("TEV_PN_EVLR_ESTM_SBJT".equals(tableNm) && "DEL".equals(type)) {
			sql = deleteIncrement_TEV_PN_EVLR_ESTM_SBJTDelList();
		}else if("TEV_PN_EVLR_ESTM_SBJT".equals(tableNm) && "TMP".equals(type)) {
			sql = deleteIncrement_TEV_PN_EVLR_ESTM_SBJTTmpList();
		}else if("KCDM312".equals(tableNm) && "DEL".equals(type)) {
			sql = deleteIncrement_KCDM312DelList();
		}else if("KCDM312".equals(tableNm) && "TMP".equals(type)) {
			sql = deleteIncrement_KCDM312TmpList();
		}else if("KCDM310".equals(tableNm) && "DEL".equals(type)) {
			sql = deleteIncrement_KCDM310DelList();
		}else if("KCDM310".equals(tableNm) && "TMP".equals(type)) {
			sql = deleteIncrement_KCDM310TmpList();
		}else if("KCDD214".equals(tableNm) && "DEL".equals(type)) {
			sql = deleteIncrement_KCDD214DelList();
		}else if("KCDD214".equals(tableNm) && "TMP".equals(type)) {
			sql = deleteIncrement_KCDD214TmpList();
		}else if("NRF_BIG_SCJNL_INFO_ITG".equals(tableNm) && "DEL".equals(type)) {
			sql = deleteIncrement_NRF_BIG_SCJNL_INFO_ITGDelList();
		}else if("NRF_BIG_SCJNL_INFO_ITG".equals(tableNm) && "TMP".equals(type)) {
			sql = deleteIncrement_NRF_BIG_SCJNL_INFO_ITGTmpList();
		}else if("NRF_BIG_PPR_INFO_ITG".equals(tableNm) && "DEL".equals(type)) {
			sql = deleteIncrement_NRF_BIG_PPR_INFO_ITGDelList();
		}else if("NRF_BIG_PPR_INFO_ITG".equals(tableNm) && "TMP".equals(type)) {
			sql = deleteIncrement_NRF_BIG_PPR_INFO_ITGTmpList();
		}else if("NRF_BIG_PPR_ATHR_INFO_ITG".equals(tableNm) && "DEL".equals(type)) {
			sql = deleteIncrement_NRF_BIG_PPR_ATHR_INFO_ITGDelList();
		}else if("NRF_BIG_PPR_ATHR_INFO_ITG".equals(tableNm) && "TMP".equals(type)) {
			sql = deleteIncrement_NRF_BIG_PPR_ATHR_INFO_ITGTmpList();
		}
		return sql;
	}
	
	private final String lakeDbNm = "big_unity"; // lake
	
	// hive - kri
	public String getIncrement_NKRDDList(String date, String tableNm) {
		return "SELECT * FROM "+lakeDbNm+"." + tableNm + " WHERE DATE(RCV_DTTM) = '"+date+"' or DATE(PRC_DTTM) = '"+date+"'";
	}
	
	// hive -kri
	public String getIncrement_NKRDMList(String date, String tableNm) {
		return "SELECT * FROM "+lakeDbNm+"." + tableNm + " WHERE DATE(REG_DTTM) = '"+date+"' or DATE(MOD_DTTM) = '"+date+"'";
	}
	
	// hive - ernd tmp
	public String getIncrement_ERNDTMPList(String date, String tableNm) {
		return "SELECT * FROM "+lakeDbNm+"." + tableNm + " WHERE DATE(MOD_DTTM) = '"+date+"'";
	}
	
	// hive - TAC_ACP_SBJT
	public String getIncrement_TAC_ACP_SBJT_DELList(String date, String tableNm) {
		return "SELECT "+tableNm+".ACP_SBJT_NO FROM "+lakeDbNm+"." + tableNm;
	}
	
	// hive - TEV_EVLR_SBJT_ESTM
	public String getIncrement_TEV_EVLR_SBJT_ESTM_DELList(String date, String tableNm) {
		return "SELECT "+tableNm+".ACP_SBJT_NO, "+tableNm+".ESTM_STEP_NO, "+tableNm+".ESTM_PN_SNO, "+tableNm+".EVLS_NO, "+tableNm+".EVLR_NO, "+tableNm+".EVLR_SNO FROM "+lakeDbNm+"." + tableNm;
	}
	
	// hive - TSL_SLC_SBJT_RSLT
	public String getIncrement_TSL_SLC_SBJT_RSLT_DELList(String date, String tableNm) {
		return "SELECT "+tableNm+".ACP_SBJT_NO, "+tableNm+".SLC_DVS_CD, "+tableNm+".SLC_NGR FROM "+lakeDbNm+"." + tableNm;
	}
	
	// hive - TAC_KWD
	public String getIncrement_TAC_KWD_DELList(String date, String tableNm) {
		return "SELECT "+tableNm+".ACP_SBJT_NO, "+tableNm+".KR_ENG_DVS_CD, "+tableNm+".KWD_SNO FROM "+lakeDbNm+"." + tableNm;
	}
	
	// hive - TCM_FILE_DTL
	public String getIncrement_TCM_FILE_DTL_DELList(String date, String tableNm) {
		return "SELECT "+tableNm+".FILE_NO, "+tableNm+".FILE_SNO FROM "+lakeDbNm+"." + tableNm;
	}
	
	// hive - TAC_SBJT_TPI_HR
	public String getIncrement_TAC_SBJT_TPI_HR_DELList(String date, String tableNm) {
		return "SELECT "+tableNm+".ACP_SBJT_NO, "+tableNm+".TPI_HR_SNO FROM "+lakeDbNm+"." + tableNm;
	}
	
	// hive - TAC_SMMR_CNTN
	public String getIncrement_TAC_SMMR_CNTN_DELList(String date, String tableNm) {
		return "SELECT "+tableNm+".ACP_SBJT_NO, "+tableNm+".KR_ENG_DVS_CD FROM "+lakeDbNm+"." + tableNm;
	}
	
	// hive - TRT_PPR
	public String getIncrement_TRT_PPR_DELList(String date, String tableNm) {
		return "SELECT "+tableNm+".STEP_ANU_SBJT_NO, "+tableNm+".OTC_YR, "+tableNm+".OTC_SNO FROM "+lakeDbNm+"." + tableNm;
	}
	
	// hive - TEV_PN_EVLR_ESTM_SBJT
	public String getIncrement_TEV_PN_EVLR_ESTM_SBJT_DELList(String date, String tableNm) {
		return "SELECT "+tableNm+".ACP_SBJT_NO, "+tableNm+".ESTM_STEP_NO, "+tableNm+".ESTM_PN_SNO, "+tableNm+".EVLR_NO, "+tableNm+".EVLR_SNO FROM "+lakeDbNm+"." + tableNm;
	}
	
	// hive - KCDM312 del
	public String getIncrement_KCDM312_DELList(String date, String tableNm) {
		return "SELECT "+tableNm+".M312_ORTE_SERV_ID FROM "+lakeDbNm+"." + tableNm;
	}
	
	// hive - KCDM312 tmp
	public String getIncrement_KCDM312TMPList(String date, String tableNm) {
		return "select * from "+lakeDbNm+"." + tableNm + " where substr(m312_resi_dt,0,8) = '"+date+"'";
	}
	
	// hive - KCDM310 del
	public String getIncrement_KCDM310_DELList(String date, String tableNm) {
		return "SELECT "+tableNm+".M310_ARTI_ID FROM "+lakeDbNm+"." + tableNm;
	}
	
	// hive - KCDM310 tmp
	public String getIncrement_KCDM310TMPList(String date, String tableNm) {
		return "select * from "+lakeDbNm+"." + tableNm + " where substr(M310_RESI_DT,0,8) = '"+date+"' or substr(M310_UPDATE_DT, 0, 8) = '"+date+"'";
	}
	
	// hive - KCDD214 del
	public String getIncrement_KCDD214_DELList(String date, String tableNm) {
		return "SELECT "+tableNm+".M140_SERE_ID, "+tableNm+".M300_VOL_ISSE_ID, "+tableNm+".M310_ARTI_ID FROM "+lakeDbNm+"." + tableNm;
	}
	
	// hive - KCDD214 tmp
	public String getIncrement_KCDD214TMPList(String date, String tableNm) {
		return "select * from "+lakeDbNm+"." + tableNm + " where substr(d214_resi_dt,0,10) = '"+date+"' or substr(d214_resu_dt, 0, 10) = '"+date+"'";
	}
	
	// hive - NRF_BIG_SCJNL_INFO_ITG del
	public String getIncrement_NRF_BIG_SCJNL_INFO_ITG_DELList(String date, String tableNm) {
		return "SELECT "+tableNm+".AGC_ID, "+tableNm+".RSCHR_REG_NO, "+tableNm+".MNG_NO "+" FROM "+lakeDbNm+"." + tableNm;
	}
	
	// hive - NRF_BIG_SCJNL_INFO_ITG tmp
	public String getIncrement_NRF_BIG_SCJNL_INFO_ITGTMPList(String date, String tableNm) {
		return "select * from "+lakeDbNm+"." + tableNm + " where substr(reg_dttm,0,10) = '"+date+"' or substr(mod_dttm, 0, 10) = '"+date+"'";
	}
	
	// hive - NRF_BIG_PPR_INFO_ITG del
	public String getIncrement_NRF_BIG_PPR_INFO_ITG_DELList(String date, String tableNm) {
		return "SELECT "+tableNm+".RSCHR_REG_NO, "+tableNm+".MNG_NO "+" FROM "+lakeDbNm+"." + tableNm;
	}
	
	// hive - NRF_BIG_PPR_INFO_ITG tmp
	public String getIncrement_NRF_BIG_PPR_INFO_ITGTMPList(String date, String tableNm) {
		return "select * from "+lakeDbNm+"." + tableNm + " where substr(mod_dttm, 0, 10) = '"+date+"'";
	}
	
	// hive - NRF_BIG_PPR_ATHR_INFO_ITG del
	public String getIncrement_NRF_BIG_PPR_ATHR_INFO_ITG_DELList(String date, String tableNm) {
		return "SELECT "+tableNm+".RSCHR_REG_NO, "+tableNm+".MNG_NO, "+tableNm+".SEQ_NO, "+tableNm+".PCN_RSCHR_REG_NO, "+tableNm+".M330_CRET_ID, "+tableNm+".D311_ARTI_CRET_ID, "+
				tableNm+".M310_ARTI_ID, "+tableNm+".REG_DTTM "+" FROM "+lakeDbNm+"." + tableNm;
	}
	
	// hive - NRF_BIG_PPR_ATHR_INFO_ITG tmp
	public String getIncrement_NRF_BIG_PPR_ATHR_INFO_ITGTMPList(String date, String tableNm) {
		return "select * from "+lakeDbNm+"." + tableNm + " where substr(reg_dttm,0,10) = '"+date+"' or substr(mod_dttm, 0, 10) = '"+date+"'";
	}
	
	// tibero
	public String deleteIncrement_NKRDD305List() {
		return "delete\r\n" + 
				"from DATAMART.NKRDD305\r\n" + 
				"where AGC_ID || RSCHR_REG_NO || MNG_NO in\r\n" + 
				"(\r\n" + 
				"select\r\n" + 
				"b.AGC_ID || b.RSCHR_REG_NO || b.MNG_NO\r\n" + 
				"from DATAMART.NKRDD305_TMP b\r\n" + 
				"where AGC_ID = b.AGC_ID\r\n" + 
				"and RSCHR_REG_NO = b.RSCHR_REG_NO\r\n" + 
				"and MNG_NO = b.MNG_NO\r\n" + 
				")";
	}
	
	// tibero
	public String deleteIncrement_NKRDD306List() {
		return "delete\r\n" + 
				"from DATAMART.NKRDD306\r\n" + 
				"where AGC_ID || RSCHR_REG_NO || MNG_NO || SEQ_NO in\r\n" + 
				"(\r\n" + 
				"select\r\n" + 
				"b.AGC_ID || b.RSCHR_REG_NO || b.MNG_NO || b.SEQ_NO\r\n" + 
				"from DATAMART.NKRDD306_TMP b\r\n" + 
				"where AGC_ID = b.AGC_ID\r\n" + 
				"and RSCHR_REG_NO = b.RSCHR_REG_NO\r\n" + 
				"and MNG_NO = b.MNG_NO\r\n" + 
				"and SEQ_NO = b.SEQ_NO\r\n" + 
				")";
	}
	
	// tibero
	public String deleteIncrement_NKRDD312List() {
		return "delete\r\n" + 
				"from DATAMART.NKRDD312\r\n" + 
				"where AGC_ID || RSCHR_REG_NO || MNG_NO || SEQ_NO in\r\n" + 
				"(\r\n" + 
				"select\r\n" + 
				"b.AGC_ID || b.RSCHR_REG_NO || b.MNG_NO || b.SEQ_NO\r\n" + 
				"from DATAMART.NKRDD312_TMP b\r\n" + 
				"where AGC_ID = b.AGC_ID\r\n" + 
				"and RSCHR_REG_NO = b.RSCHR_REG_NO\r\n" + 
				"and MNG_NO = b.MNG_NO\r\n" + 
				"and SEQ_NO = b.SEQ_NO\r\n" + 
				")";
	}
	
	// tibero
	public String deleteIncrement_NKRDD212List() {
		return "delete\r\n" + 
				"from DATAMART.NKRDD212\r\n" + 
				"where RSCHR_REG_NO || MNG_NO || SEQ_NO in\r\n" + 
				"(\r\n" + 
				"select\r\n" + 
				"b.RSCHR_REG_NO || b.MNG_NO || b.SEQ_NO\r\n" + 
				"from DATAMART.NKRDD212_TMP b\r\n" + 
				"where RSCHR_REG_NO = b.RSCHR_REG_NO\r\n" + 
				"and MNG_NO = b.MNG_NO\r\n" + 
				"and SEQ_NO = b.SEQ_NO\r\n" + 
				")";
	}
	
	// tibero
	public String deleteIncrement_NKRDD206List() {
		return "delete \r\n" + 
				"from DATAMART.NKRDD206\r\n" + 
				"where RSCHR_REG_NO || MNG_NO || SEQ_NO in \r\n" + 
				"(\r\n" + 
				"select \r\n" + 
				"b.RSCHR_REG_NO || b.MNG_NO || b.SEQ_NO\r\n" + 
				"from DATAMART.NKRDD206_TMP b \r\n" + 
				"where RSCHR_REG_NO = b.RSCHR_REG_NO\r\n" + 
				"and MNG_NO = b.MNG_NO\r\n" + 
				"and SEQ_NO = b.SEQ_NO\r\n" + 
				")";
	}
	
	// tibero
	public String deleteIncrement_NKRDD205List() {
		return "delete \r\n" + 
				"from DATAMART.NKRDD205\r\n" + 
				"where RSCHR_REG_NO || MNG_NO in \r\n" + 
				"(\r\n" + 
				"select \r\n" + 
				"b.RSCHR_REG_NO || b.MNG_NO\r\n" + 
				"from DATAMART.NKRDD205_TMP b \r\n" + 
				"where RSCHR_REG_NO = b.RSCHR_REG_NO\r\n" + 
				"and MNG_NO = b.MNG_NO\r\n" + 
				")";
	}
	
	// tibero
	public String deleteIncrement_NKRDD215List() {
		return "delete \r\n" + 
				"from DATAMART.NKRDD215\r\n" + 
				"where RSCHR_REG_NO || MNG_NO in \r\n" + 
				"(\r\n" + 
				"select \r\n" + 
				"b.RSCHR_REG_NO || b.MNG_NO\r\n" + 
				"from DATAMART.NKRDD215_TMP b \r\n" + 
				"where RSCHR_REG_NO = b.RSCHR_REG_NO\r\n" + 
				"and MNG_NO = b.MNG_NO\r\n" + 
				")";
	}
	
	// tibero
	public String deleteIncrement_NKRDD204List() {
		return "delete \r\n" + 
				"from DATAMART.NKRDD204\r\n" + 
				"where RSCHR_REG_NO || MNG_NO in \r\n" + 
				"(\r\n" + 
				"select \r\n" + 
				"b.RSCHR_REG_NO || b.MNG_NO\r\n" + 
				"from DATAMART.NKRDD204_TMP b \r\n" + 
				"where RSCHR_REG_NO = b.RSCHR_REG_NO\r\n" + 
				"and MNG_NO = b.MNG_NO\r\n" + 
				")";
	}
	
	// tibero
	public String deleteIncrement_NKRDD208List() {
		return "delete \r\n" + 
				"from DATAMART.NKRDD208\r\n" + 
				"where RSCHR_REG_NO || MNG_NO in \r\n" + 
				"(\r\n" + 
				"select \r\n" + 
				"b.RSCHR_REG_NO || b.MNG_NO\r\n" + 
				"from DATAMART.NKRDD208_TMP b \r\n" + 
				"where RSCHR_REG_NO = b.RSCHR_REG_NO\r\n" + 
				"and MNG_NO = b.MNG_NO\r\n" + 
				")";
	}
	
	// tibero
	public String deleteIncrement_NKRDM201List() {
		return "delete \r\n" + 
				"from DATAMART.NKRDM201\r\n" + 
				"where RSCHR_REG_NO in \r\n" + 
				"(\r\n" + 
				"select \r\n" + 
				"b.RSCHR_REG_NO\r\n" + 
				"from DATAMART.NKRDM201_TMP b \r\n" + 
				"where RSCHR_REG_NO = b.RSCHR_REG_NO\r\n" + 
				")";
	}
	
	// tibero
	public String deleteIncrement_NKRDD315List() {
		return "delete\r\n" + 
				"from DATAMART.NKRDD315\r\n" + 
				"where AGC_ID || RSCHR_REG_NO || MNG_NO in\r\n" + 
				"(\r\n" + 
				"select\r\n" + 
				"b.AGC_ID || b.RSCHR_REG_NO || b.MNG_NO\r\n" + 
				"from DATAMART.NKRDD315_TMP b\r\n" + 
				"where AGC_ID = b.AGC_ID\r\n" + 
				"and RSCHR_REG_NO = b.RSCHR_REG_NO\r\n" + 
				"and MNG_NO = b.MNG_NO\r\n" + 
				")";
	}
	
	// tibero
	public String deleteIncrement_NKRDD311List() {
		return "delete\r\n" + 
				"from DATAMART.NKRDD311\r\n" + 
				"where AGC_ID || RSCHR_REG_NO || MNG_NO in\r\n" + 
				"(\r\n" + 
				"select\r\n" + 
				"b.AGC_ID || b.RSCHR_REG_NO || b.MNG_NO\r\n" + 
				"from DATAMART.NKRDD311_TMP b\r\n" + 
				"where AGC_ID = b.AGC_ID\r\n" + 
				"and RSCHR_REG_NO = b.RSCHR_REG_NO\r\n" + 
				"and MNG_NO = b.MNG_NO\r\n" + 
				")";
	}
	
	// tibero
	public String deleteIncrement_NKRDD310List() {
		return "delete\r\n" + 
				"from DATAMART.NKRDD310\r\n" + 
				"where AGC_ID || RSCHR_REG_NO || MNG_NO || SEQ_NO in\r\n" + 
				"(\r\n" + 
				"select\r\n" + 
				"b.AGC_ID || b.RSCHR_REG_NO || b.MNG_NO || b.SEQ_NO\r\n" + 
				"from DATAMART.NKRDD310_TMP b\r\n" + 
				"where AGC_ID = b.AGC_ID\r\n" + 
				"and RSCHR_REG_NO = b.RSCHR_REG_NO\r\n" + 
				"and MNG_NO = b.MNG_NO\r\n" + 
				"and SEQ_NO = b.SEQ_NO\r\n" + 
				")";
	}
	
	// tibero
	public String deleteIncrement_NKRDD210List() {
		return "delete\r\n" + 
				"from DATAMART.NKRDD210\r\n" + 
				"where RSCHR_REG_NO || MNG_NO || SEQ_NO in\r\n" + 
				"(\r\n" + 
				"select\r\n" + 
				"b.RSCHR_REG_NO || b.MNG_NO || b.SEQ_NO\r\n" + 
				"from DATAMART.NKRDD210_TMP b\r\n" + 
				"where RSCHR_REG_NO = b.RSCHR_REG_NO\r\n" + 
				"and MNG_NO = b.MNG_NO\r\n" + 
				"and SEQ_NO = b.SEQ_NO\r\n" + 
				")";
	}
	
	// tibero
	public String deleteIncrement_NKRDD309List() {
		return "delete\r\n" + 
				"from DATAMART.NKRDD309\r\n" + 
				"where AGC_ID || MNG_NO || RSCHR_REG_NO || SEQ_NO in\r\n" + 
				"(\r\n" + 
				"select\r\n" + 
				"b.AGC_ID || b.MNG_NO || b.RSCHR_REG_NO || b.SEQ_NO\r\n" + 
				"from DATAMART.NKRDD309_TMP b\r\n" + 
				"where AGC_ID = b.AGC_ID\r\n" + 
				"and MNG_NO = b.MNG_NO\r\n" + 
				"and RSCHR_REG_NO = b.RSCHR_REG_NO\r\n" + 
				"and SEQ_NO = b.SEQ_NO\r\n" + 
				")";
	}
	
	// tibero
	public String deleteIncrement_NKRDD209List() {
		return "delete\r\n" + 
				"from DATAMART.NKRDD209\r\n" + 
				"where RSCHR_REG_NO || MNG_NO || SEQ_NO in\r\n" + 
				"(\r\n" + 
				"select\r\n" + 
				"b.RSCHR_REG_NO || b.MNG_NO || b.SEQ_NO\r\n" + 
				"from DATAMART.NKRDD209_TMP b\r\n" + 
				"where RSCHR_REG_NO = b.RSCHR_REG_NO\r\n" + 
				"and MNG_NO = b.MNG_NO\r\n" + 
				"and SEQ_NO = b.SEQ_NO\r\n" + 
				")";
	}
	
	// tibero
	public String deleteIncrement_NKRDD303List() {
		return "delete\r\n" + 
				"from DATAMART.NKRDD303\r\n" + 
				"where AGC_ID || MNG_NO || RSCHR_REG_NO in\r\n" + 
				"(\r\n" + 
				"select\r\n" + 
				"b.AGC_ID || b.MNG_NO || b.RSCHR_REG_NO\r\n" + 
				"from DATAMART.NKRDD303_TMP b\r\n" + 
				"where AGC_ID = b.AGC_ID\r\n" + 
				"and MNG_NO = b.MNG_NO\r\n" + 
				"and RSCHR_REG_NO = b.RSCHR_REG_NO\r\n" + 
				")";
	}
	
	// tibero
	public String deleteIncrement_NKRDD203List() {
		return "delete\r\n" + 
				"from DATAMART.NKRDD203\r\n" + 
				"where RSCHR_REG_NO || MNG_NO in\r\n" + 
				"(\r\n" + 
				"select\r\n" + 
				"b.RSCHR_REG_NO || b.MNG_NO\r\n" + 
				"from DATAMART.NKRDD203_TMP b\r\n" + 
				"where RSCHR_REG_NO = b.RSCHR_REG_NO\r\n" + 
				"and MNG_NO = b.MNG_NO\r\n" + 
				")";
	}
	
	// tibero
	public String deleteIncrement_NKRDM279List() {
		return "delete\r\n" + 
				"from DATAMART.NKRDM279\r\n" + 
				"where KOMARC_CONTROL_NO in\r\n" + 
				"(\r\n" + 
				"select\r\n" + 
				"b.KOMARC_CONTROL_NO\r\n" + 
				"from DATAMART.NKRDM279_TMP b\r\n" + 
				"where KOMARC_CONTROL_NO = b.KOMARC_CONTROL_NO\r\n" + 
				")";
	}
	
	// tibero
	public String deleteIncrement_NKRDM270List() {
		return "delete\r\n" + 
				"from DATAMART.NKRDM270\r\n" + 
				"where ITG_PPR_ID in\r\n" + 
				"(\r\n" + 
				"select\r\n" + 
				"b.ITG_PPR_ID\r\n" + 
				"from DATAMART.NKRDM270_TMP b\r\n" + 
				"where ITG_PPR_ID = b.ITG_PPR_ID\r\n" + 
				")";
	}
	
	// tibero
	public String deleteIncrement_NKRDD202List() {
		return "delete\r\n" + 
				"from DATAMART.NKRDD202\r\n" + 
				"where RSCHR_REG_NO || MNG_NO in\r\n" + 
				"(\r\n" + 
				"select\r\n" + 
				"b.RSCHR_REG_NO || b.MNG_NO\r\n" + 
				"from DATAMART.NKRDD202_TMP b\r\n" + 
				"where RSCHR_REG_NO = b.RSCHR_REG_NO\r\n" + 
				"and MNG_NO = b.MNG_NO\r\n" + 
				")";
	}
	
	// tibero
	public String deleteIncrement_NKRDD302List() {
		return "delete\r\n" + 
				"from DATAMART.NKRDD302\r\n" + 
				"where AGC_ID || MNG_NO || RSCHR_REG_NO in\r\n" + 
				"(\r\n" + 
				"select\r\n" + 
				"b.AGC_ID || b.MNG_NO || b.RSCHR_REG_NO\r\n" + 
				"from DATAMART.NKRDD302_TMP b\r\n" + 
				"where AGC_ID = b.AGC_ID\r\n" + 
				"and MNG_NO = b.MNG_NO\r\n" + 
				"and RSCHR_REG_NO = b.RSCHR_REG_NO\r\n" + 
				")";
	}
	
	// tibero
	public String deleteIncrement_NKRDD304List() {
		return "delete\r\n" + 
				"from DATAMART.NKRDD304\r\n" + 
				"where AGC_ID || MNG_NO || RSCHR_REG_NO in\r\n" + 
				"(\r\n" + 
				"select\r\n" + 
				"b.AGC_ID || b.MNG_NO || b.RSCHR_REG_NO\r\n" + 
				"from DATAMART.NKRDD304_TMP b\r\n" + 
				"where AGC_ID = b.AGC_ID\r\n" + 
				"and MNG_NO = b.MNG_NO\r\n" + 
				"and RSCHR_REG_NO = b.RSCHR_REG_NO\r\n" + 
				")";
	}
	
	// tibero
	public String deleteIncrement_NKRDD308List() {
		return "delete\r\n" + 
				"from DATAMART.NKRDD308\r\n" + 
				"where AGC_ID || MNG_NO || RSCHR_REG_NO in\r\n" + 
				"(\r\n" + 
				"select\r\n" + 
				"b.AGC_ID || b.MNG_NO || b.RSCHR_REG_NO\r\n" + 
				"from DATAMART.NKRDD308_TMP b\r\n" + 
				"where AGC_ID = b.AGC_ID\r\n" + 
				"and MNG_NO = b.MNG_NO\r\n" + 
				"and RSCHR_REG_NO = b.RSCHR_REG_NO\r\n" + 
				")";
	}
	
	// tibero
	public String deleteIncrement_TAC_ACP_SBJTDelList() {
		return "delete\r\n" + 
				"from DATAMART.TAC_ACP_SBJT\r\n" + 
				"where ACP_SBJT_NO not in\r\n" + 
				"(\r\n" + 
				"select\r\n" + 
				"b.ACP_SBJT_NO \r\n" + 
				"from DATAMART.TAC_ACP_SBJT_DEL b\r\n" + 
				"where ACP_SBJT_NO = b.ACP_SBJT_NO\r\n" + 
				")";
	}
	
	// tibero
	public String deleteIncrement_TAC_ACP_SBJTTmpList() {
		return "delete\r\n" + 
				"from DATAMART.TAC_ACP_SBJT\r\n" + 
				"where ACP_SBJT_NO in\r\n" + 
				"(\r\n" + 
				"select\r\n" + 
				"b.ACP_SBJT_NO \r\n" + 
				"from DATAMART.TAC_ACP_SBJT_TMP b\r\n" + 
				"where ACP_SBJT_NO = b.ACP_SBJT_NO\r\n" + 
				")";
	}
	
	// tibero
	public String deleteIncrement_TEV_EVLR_SBJT_ESTMDelList() {
		return "delete \r\n" + 
				"from DATAMART.TEV_EVLR_SBJT_ESTM\r\n" + 
				" where ACP_SBJT_NO || ESTM_STEP_NO || ESTM_PN_SNO || EVLS_NO || EVLR_NO || EVLR_SNO not in \r\n" + 
				"(\r\n" + 
				"select \r\n" + 
				"b.ACP_SBJT_NO || b.ESTM_STEP_NO || b.ESTM_PN_SNO || b.EVLS_NO || b.EVLR_NO || b.EVLR_SNO\r\n" + 
				"from DATAMART.TEV_EVLR_SBJT_ESTM_DEL b\r\n" + 
				"where 1=1 \r\n" + 
				"and ACP_SBJT_NO = b.ACP_SBJT_NO\r\n" + 
				"and ESTM_STEP_NO = b.ESTM_STEP_NO\r\n" + 
				"and ESTM_PN_SNO = b.ESTM_PN_SNO\r\n" + 
				"and EVLS_NO = b.EVLS_NO\r\n" + 
				"and EVLR_NO = b.EVLR_NO\r\n" + 
				"and EVLR_SNO = b.EVLR_SNO\r\n" + 
				")";
	}
	
	// tibero
	public String deleteIncrement_TEV_EVLR_SBJT_ESTMTmpList() {
		return "delete \r\n" + 
				"from DATAMART.TEV_EVLR_SBJT_ESTM\r\n" + 
				" where ACP_SBJT_NO || ESTM_STEP_NO || ESTM_PN_SNO || EVLS_NO || EVLR_NO || EVLR_SNO in \r\n" + 
				"(\r\n" + 
				"select \r\n" + 
				"b.ACP_SBJT_NO || b.ESTM_STEP_NO || b.ESTM_PN_SNO || b.EVLS_NO || b.EVLR_NO || b.EVLR_SNO\r\n" + 
				"from DATAMART.TEV_EVLR_SBJT_ESTM_TMP b\r\n" + 
				"where 1=1 \r\n" + 
				"and ACP_SBJT_NO = b.ACP_SBJT_NO\r\n" + 
				"and ESTM_STEP_NO = b.ESTM_STEP_NO\r\n" + 
				"and ESTM_PN_SNO = b.ESTM_PN_SNO\r\n" + 
				"and EVLS_NO = b.EVLS_NO\r\n" + 
				"and EVLR_NO = b.EVLR_NO\r\n" + 
				"and EVLR_SNO = b.EVLR_SNO\r\n" + 
				")";
	}
	
	// tibero
	public String deleteIncrement_TSL_SLC_SBJT_RSLTDelList() {
		return "delete \r\n" + 
				"from DATAMART.TSL_SLC_SBJT_RSLT\r\n" + 
				" where ACP_SBJT_NO || SLC_DVS_CD || SLC_NGR not in \r\n" + 
				"(\r\n" + 
				"select \r\n" + 
				"b.ACP_SBJT_NO || b.SLC_DVS_CD || b.SLC_NGR\r\n" + 
				"from DATAMART.TSL_SLC_SBJT_RSLT_DEL b\r\n" + 
				"where 1=1 \r\n" + 
				"and ACP_SBJT_NO = b.ACP_SBJT_NO\r\n" + 
				"and SLC_DVS_CD = b.SLC_DVS_CD\r\n" + 
				"and SLC_NGR = b.SLC_NGR\r\n" + 
				")";
	}
	
	// tibero
	public String deleteIncrement_TSL_SLC_SBJT_RSLTTmpList() {
		return "delete \r\n" + 
				"from DATAMART.TSL_SLC_SBJT_RSLT\r\n" + 
				" where ACP_SBJT_NO || SLC_DVS_CD || SLC_NGR in \r\n" + 
				"(\r\n" + 
				"select \r\n" + 
				"b.ACP_SBJT_NO || b.SLC_DVS_CD || b.SLC_NGR\r\n" + 
				"from DATAMART.TSL_SLC_SBJT_RSLT_TMP b\r\n" + 
				"where 1=1 \r\n" + 
				"and ACP_SBJT_NO = b.ACP_SBJT_NO\r\n" + 
				"and SLC_DVS_CD = b.SLC_DVS_CD\r\n" + 
				"and SLC_NGR = b.SLC_NGR\r\n" + 
				")";
	}
	
	// tibero
	public String deleteIncrement_TAC_KWDDelList() {
		return "delete\r\n" + 
				"from DATAMART.TAC_KWD\r\n" + 
				"where ACP_SBJT_NO || KR_ENG_DVS_CD || KWD_SNO not in\r\n" + 
				"(\r\n" + 
				"select\r\n" + 
				"b.ACP_SBJT_NO || b.KR_ENG_DVS_CD || b.KWD_SNO\r\n" + 
				"from DATAMART.TAC_KWD_DEL b\r\n" + 
				"where ACP_SBJT_NO = b.ACP_SBJT_NO\r\n" + 
				"and KR_ENG_DVS_CD = b.KR_ENG_DVS_CD\r\n" + 
				"and KWD_SNO = b.KWD_SNO\r\n" + 
				")";
	}
	
	// tibero
	public String deleteIncrement_TAC_KWDTmpList() {
		return "delete \r\n" + 
				"from DATAMART.TAC_KWD\r\n" + 
				"where ACP_SBJT_NO || KR_ENG_DVS_CD || KWD_SNO in \r\n" + 
				"(\r\n" + 
				"select \r\n" + 
				"b.ACP_SBJT_NO || b.KR_ENG_DVS_CD || b.KWD_SNO\r\n" + 
				"from DATAMART.TAC_KWD_TMP b \r\n" + 
				"where ACP_SBJT_NO = b.ACP_SBJT_NO\r\n" + 
				"and KR_ENG_DVS_CD = b.KR_ENG_DVS_CD\r\n" + 
				"and KWD_SNO = b.KWD_SNO\r\n" + 
				")";
	}
	
	// tibero
	public String deleteIncrement_TCM_FILE_DTLDelList() {
		return "delete\r\n" + 
				"from DATAMART.TCM_FILE_DTL\r\n" + 
				"where FILE_NO || FILE_SNO not in\r\n" + 
				"(\r\n" + 
				"select\r\n" + 
				"b.FILE_NO || b.FILE_SNO\r\n" + 
				"from DATAMART.TCM_FILE_DTL_DEL b\r\n" + 
				"where FILE_NO= b.FILE_NO\r\n" + 
				"and FILE_SNO = b.FILE_SNO\r\n" + 
				")";
	}
	
	// tibero
	public String deleteIncrement_TCM_FILE_DTLTmpList() {
		return "delete \r\n" + 
				"from DATAMART.TCM_FILE_DTL\r\n" + 
				"where FILE_NO || FILE_SNO in \r\n" + 
				"(\r\n" + 
				"select \r\n" + 
				"b.FILE_NO || b.FILE_SNO\r\n" + 
				"from DATAMART.TCM_FILE_DTL_TMP b \r\n" + 
				"where FILE_NO= b.FILE_NO\r\n" + 
				"and FILE_SNO = b.FILE_SNO\r\n" + 
				")";
	}
	
	// tibero
	public String deleteIncrement_TAC_SBJT_TPI_HRDelList() {
		return "delete\r\n" + 
				"from DATAMART.TAC_SBJT_TPI_HR\r\n" + 
				"where ACP_SBJT_NO || TPI_HR_SNO not in\r\n" + 
				"(\r\n" + 
				"select\r\n" + 
				"b.ACP_SBJT_NO || b.TPI_HR_SNO\r\n" + 
				"from DATAMART.TAC_SBJT_TPI_HR_DEL b\r\n" + 
				"where ACP_SBJT_NO = b.ACP_SBJT_NO\r\n" + 
				"and TPI_HR_SNO = b.TPI_HR_SNO\r\n" + 
				")";
	}
	
	// tibero
	public String deleteIncrement_TAC_SBJT_TPI_HRTmpList() {
		return "delete \r\n" + 
				"from DATAMART.TAC_SBJT_TPI_HR\r\n" + 
				"where ACP_SBJT_NO || TPI_HR_SNO in \r\n" + 
				"(\r\n" + 
				"select \r\n" + 
				"b.ACP_SBJT_NO || b.TPI_HR_SNO\r\n" + 
				"from DATAMART.TAC_SBJT_TPI_HR_TMP b \r\n" + 
				"where ACP_SBJT_NO = b.ACP_SBJT_NO\r\n" + 
				"and TPI_HR_SNO = b.TPI_HR_SNO\r\n" + 
				")";
	}
	
	// tibero
	public String deleteIncrement_TAC_SMMR_CNTNDelList() {
		return "delete\r\n" + 
				"from DATAMART.TAC_SMMR_CNTN\r\n" + 
				"where ACP_SBJT_NO || KR_ENG_DVS_CD not in\r\n" + 
				"(\r\n" + 
				"select\r\n" + 
				"b.ACP_SBJT_NO || b.KR_ENG_DVS_CD\r\n" + 
				"from DATAMART.TAC_SMMR_CNTN_DEL b\r\n" + 
				"where ACP_SBJT_NO = b.ACP_SBJT_NO\r\n" + 
				"and KR_ENG_DVS_CD = b.KR_ENG_DVS_CD\r\n" + 
				")";
	}
	
	// tibero
	public String deleteIncrement_TAC_SMMR_CNTNTmpList() {
		return "delete \r\n" + 
				"from DATAMART.TAC_SMMR_CNTN\r\n" + 
				"where ACP_SBJT_NO || KR_ENG_DVS_CD in \r\n" + 
				"(\r\n" + 
				"select \r\n" + 
				"b.ACP_SBJT_NO || b.KR_ENG_DVS_CD\r\n" + 
				"from DATAMART.TAC_SMMR_CNTN_TMP b \r\n" + 
				"where ACP_SBJT_NO = b.ACP_SBJT_NO\r\n" + 
				"and KR_ENG_DVS_CD = b.KR_ENG_DVS_CD\r\n" + 
				")";
	}
	
	// tibero
	public String deleteIncrement_TRT_PPRDelList() {
		return "delete\r\n" + 
				"from DATAMART.TRT_PPR\r\n" + 
				"where STEP_ANU_SBJT_NO || OTC_YR || OTC_SNO not in\r\n" + 
				"(\r\n" + 
				"select\r\n" + 
				"b.STEP_ANU_SBJT_NO || b.OTC_YR || b.OTC_SNO\r\n" + 
				"from DATAMART.TRT_PPR_DEL b\r\n" + 
				"where STEP_ANU_SBJT_NO = b.STEP_ANU_SBJT_NO\r\n" + 
				"and OTC_YR = b.OTC_YR\r\n" + 
				"and OTC_SNO = b.OTC_SNO\r\n" + 
				")";
	}
	
	// tibero
	public String deleteIncrement_TRT_PPRTmpList() {
		return "delete \r\n" + 
				"from DATAMART.TRT_PPR\r\n" + 
				"where STEP_ANU_SBJT_NO || OTC_YR || OTC_SNO in \r\n" + 
				"(\r\n" + 
				"select \r\n" + 
				"b.STEP_ANU_SBJT_NO || b.OTC_YR || b.OTC_SNO\r\n" + 
				"from DATAMART.TRT_PPR_TMP b \r\n" + 
				"where STEP_ANU_SBJT_NO = b.STEP_ANU_SBJT_NO\r\n" + 
				"and OTC_YR = b.OTC_YR\r\n" + 
				"and OTC_SNO = b.OTC_SNO\r\n" + 
				")";
	}
	
	// tibero
	public String deleteIncrement_TEV_PN_EVLR_ESTM_SBJTDelList() {
		return "delete \r\n" + 
				"from DATAMART.TEV_PN_EVLR_ESTM_SBJT\r\n" + 
				" where ACP_SBJT_NO || ESTM_STEP_NO || ESTM_PN_SNO || EVLR_NO || EVLR_SNO not in \r\n" + 
				"(\r\n" + 
				"select \r\n" + 
				"b.ACP_SBJT_NO || b.ESTM_STEP_NO || b.ESTM_PN_SNO || b.EVLR_NO || b.EVLR_SNO\r\n" + 
				"from DATAMART.TEV_PN_EVLR_ESTM_SBJT_DEL b\r\n" + 
				"where 1=1 \r\n" + 
				"and ACP_SBJT_NO = b.ACP_SBJT_NO\r\n" + 
				"and ESTM_STEP_NO = b.ESTM_STEP_NO\r\n" + 
				"and ESTM_PN_SNO = b.ESTM_PN_SNO\r\n" + 
				"and EVLR_NO = b.EVLR_NO\r\n" + 
				"and EVLR_SNO = b.EVLR_SNO\r\n" + 
				")";
	}
	
	// tibero
	public String deleteIncrement_TEV_PN_EVLR_ESTM_SBJTTmpList() {
		return "delete \r\n" + 
				"from DATAMART.TEV_PN_EVLR_ESTM_SBJT\r\n" + 
				" where ACP_SBJT_NO || ESTM_STEP_NO || ESTM_PN_SNO || EVLR_NO || EVLR_SNO in \r\n" + 
				"(\r\n" + 
				"select \r\n" + 
				"b.ACP_SBJT_NO || b.ESTM_STEP_NO || b.ESTM_PN_SNO || b.EVLR_NO || b.EVLR_SNO\r\n" + 
				"from DATAMART.TEV_PN_EVLR_ESTM_SBJT_TMP b\r\n" + 
				"where 1=1 \r\n" + 
				"and ACP_SBJT_NO = b.ACP_SBJT_NO\r\n" + 
				"and ESTM_STEP_NO = b.ESTM_STEP_NO\r\n" + 
				"and ESTM_PN_SNO = b.ESTM_PN_SNO\r\n" + 
				"and EVLR_NO = b.EVLR_NO\r\n" + 
				"and EVLR_SNO = b.EVLR_SNO\r\n" + 
				")";
	}
	
	// tibero
	public String deleteIncrement_KCDM312DelList() {
		return "delete\r\n" + 
				"from DATAMART.KCDM312\r\n" + 
				"where M312_ORTE_SERV_ID not in \r\n" + 
				"(\r\n" + 
				"select \r\n" + 
				"b.M312_ORTE_SERV_ID\r\n" + 
				"from DATAMART.KCDM312_DEL b\r\n" + 
				"where M312_ORTE_SERV_ID = b.M312_ORTE_SERV_ID\r\n" + 
				")";
	}
	
	// tibero
	public String deleteIncrement_KCDM312TmpList() {
		return "delete\r\n" + 
				"from DATAMART.KCDM312\r\n" + 
				"where M312_ORTE_SERV_ID in \r\n" + 
				"(\r\n" + 
				"select \r\n" + 
				"b.M312_ORTE_SERV_ID\r\n" + 
				"from DATAMART.KCDM312_TMP b\r\n" + 
				"where M312_ORTE_SERV_ID = b.M312_ORTE_SERV_ID\r\n" + 
				")";
	}
	
	// tibero
	public String deleteIncrement_KCDM310DelList() {
		return "delete\r\n" + 
				"from DATAMART.KCDM310\r\n" + 
				"where M310_ARTI_ID not in\r\n" + 
				"(\r\n" + 
				"select\r\n" + 
				"b.M310_ARTI_ID\r\n" + 
				"from DATAMART.KCDM310_DEL b\r\n" + 
				"where M310_ARTI_ID = b.M310_ARTI_ID\r\n" + 
				")";
	}
	
	// tibero
	public String deleteIncrement_KCDM310TmpList() {
		return "delete \r\n" + 
				"from DATAMART.KCDM310\r\n" + 
				"where M310_ARTI_ID in \r\n" + 
				"(\r\n" + 
				"select \r\n" + 
				"b.M310_ARTI_ID \r\n" + 
				"from DATAMART.KCDM310_TMP b \r\n" + 
				"where M310_ARTI_ID = b.M310_ARTI_ID\r\n" + 
				")";
	}
	
	// tibero
	public String deleteIncrement_KCDD214DelList() {
		return "delete\r\n" + 
				"from DATAMART.KCDD214\r\n" + 
				" where M140_SERE_ID || M300_VOL_ISSE_ID || M310_ARTI_ID not in \r\n" + 
				"(\r\n" + 
				"select\r\n" + 
				"b.M140_SERE_ID || b.M300_VOL_ISSE_ID || b.M310_ARTI_ID \r\n" + 
				"from DATAMART.KCDD214_DEL b\r\n" + 
				"where 1=1 \r\n" + 
				"and M140_SERE_ID = b.M140_SERE_ID\r\n" + 
				"and M300_VOL_ISSE_ID = b.M300_VOL_ISSE_ID\r\n" + 
				"and M310_ARTI_ID = b.M310_ARTI_ID\r\n" + 
				")";
	}
	
	// tibero
	public String deleteIncrement_KCDD214TmpList() {
		return "delete \r\n" + 
				"from DATAMART.KCDD214\r\n" + 
				" where M140_SERE_ID || M300_VOL_ISSE_ID || M310_ARTI_ID in \r\n" + 
				"(\r\n" + 
				"select \r\n" + 
				"b.M140_SERE_ID || b.M300_VOL_ISSE_ID || b.M310_ARTI_ID \r\n" + 
				"from DATAMART.KCDD214_TMP b \r\n" + 
				"where 1=1 \r\n" + 
				"and M140_SERE_ID = b.M140_SERE_ID\r\n" + 
				"and M300_VOL_ISSE_ID = b.M300_VOL_ISSE_ID\r\n" + 
				"and M310_ARTI_ID = b.M310_ARTI_ID\r\n" + 
				")";
	}
	
	// tibero
	public String deleteIncrement_NRF_BIG_SCJNL_INFO_ITGDelList() {
		return "delete\r\n" + 
				"from (\r\n" + 
				" select a.AGC_ID , a.RSCHR_REG_NO , a.MNG_NO\r\n" + 
				" from DATAMART.NRF_BIG_SCJNL_INFO_ITG a\r\n" + 
				" where a.RSCHR_REG_NO is not null and a.MNG_NO is not null\r\n" + 
				" and not exists (  \r\n" + 
				" 	select 1\r\n" + 
				"    from DATAMART.NRF_BIG_SCJNL_INFO_ITG_DEL b\r\n" + 
				"    where b.RSCHR_REG_NO is not null and b.MNG_NO is not null\r\n" + 
				"    and a.AGC_ID = b.AGC_ID\r\n" + 
				"    and a.RSCHR_REG_NO = b.RSCHR_REG_NO\r\n" + 
				"    and a.MNG_NO = b.MNG_NO\r\n" + 
				"    )\r\n" + 
				"  )";
	}
	
	// tibero
	public String deleteIncrement_NRF_BIG_SCJNL_INFO_ITGTmpList() {
		return "delete\r\n" + 
				"from (\r\n" + 
				" select a.AGC_ID , a.RSCHR_REG_NO , a.MNG_NO\r\n" + 
				" from DATAMART.NRF_BIG_SCJNL_INFO_ITG a\r\n" + 
				" where a.RSCHR_REG_NO is not null and a.MNG_NO is not null\r\n" + 
				" and exists (  \r\n" + 
				" 	select 1\r\n" + 
				"    from DATAMART.NRF_BIG_SCJNL_INFO_ITG_TMP b\r\n" + 
				"    where b.RSCHR_REG_NO is not null and b.MNG_NO is not null\r\n" + 
				"    and a.AGC_ID = b.AGC_ID\r\n" + 
				"    and a.RSCHR_REG_NO = b.RSCHR_REG_NO\r\n" + 
				"    and a.MNG_NO = b.MNG_NO\r\n" + 
				"    )\r\n" + 
				"  )";
	}
	
	// tibero
	public String deleteIncrement_NRF_BIG_PPR_INFO_ITGDelList() {
		return "delete\r\n" + 
				"from (\r\n" + 
				" select a.RSCHR_REG_NO , a.MNG_NO\r\n" + 
				" from DATAMART.NRF_BIG_PPR_INFO_ITG a\r\n" + 
				" where a.RSCHR_REG_NO is not null and a.MNG_NO is not null\r\n" + 
				" and not exists (  \r\n" + 
				" 	select 1\r\n" + 
				"    from DATAMART.NRF_BIG_PPR_INFO_ITG_DEL b\r\n" + 
				"    where b.RSCHR_REG_NO is not null and b.MNG_NO is not null\r\n" + 
				"    and a.RSCHR_REG_NO = b.RSCHR_REG_NO\r\n" + 
				"    and a.MNG_NO = b.MNG_NO\r\n" + 
				"    )\r\n" + 
				"  )";
	}
	
	// tibero
	public String deleteIncrement_NRF_BIG_PPR_INFO_ITGTmpList() {
		return "delete\r\n" + 
				"from (\r\n" + 
				" select a.RSCHR_REG_NO , a.MNG_NO\r\n" + 
				" from DATAMART.NRF_BIG_PPR_INFO_ITG a\r\n" + 
				" where a.RSCHR_REG_NO is not null and a.MNG_NO is not null\r\n" + 
				" and exists (  \r\n" + 
				" 	select 1\r\n" + 
				"    from DATAMART.NRF_BIG_PPR_INFO_ITG_TMP b\r\n" + 
				"    where b.RSCHR_REG_NO is not null and b.MNG_NO is not null\r\n" + 
				"    and a.RSCHR_REG_NO = b.RSCHR_REG_NO\r\n" + 
				"    and a.MNG_NO = b.MNG_NO\r\n" + 
				"    )\r\n" + 
				"  )";
	}
	
	// tibero
	public String deleteIncrement_NRF_BIG_PPR_ATHR_INFO_ITGDelList() {
		return "delete\r\n" + 
				"from (\r\n" + 
				" select 1\r\n" + 
				" from DATAMART.NRF_BIG_PPR_ATHR_INFO_ITG a\r\n" + 
				" where a.PCN_RSCHR_REG_NO is not null and a.M330_CRET_ID is not null and a.D311_ARTI_CRET_ID is not null and a.M310_ARTI_ID is not null\r\n" + 
				"and not exists (  \r\n" + 
				" 	select 1\r\n" + 
				"    from DATAMART.NRF_BIG_PPR_ATHR_INFO_ITG_DEL b\r\n" + 
				"   where  b.PCN_RSCHR_REG_NO is not null and b.M330_CRET_ID is not null and b.D311_ARTI_CRET_ID is not null and b.M310_ARTI_ID is not null\r\n" + 
				"    and a.RSCHR_REG_NO = b.RSCHR_REG_NO\r\n" + 
				"    and a.MNG_NO = b.MNG_NO\r\n" + 
				"    and a.SEQ_NO = b.SEQ_NO\r\n" + 
				"    and a.PCN_RSCHR_REG_NO = b.PCN_RSCHR_REG_NO\r\n" + 
				"    and a.M330_CRET_ID = b.M330_CRET_ID\r\n" + 
				"    and a.D311_ARTI_CRET_ID = b.D311_ARTI_CRET_ID\r\n" + 
				"    and a.M310_ARTI_ID = b.M310_ARTI_ID\r\n" + 
				"    and a.REG_DTTM = b.REG_DTTM\r\n" + 
				"    )\r\n" + 
				"  )";
	}
	
	// tibero
	public String deleteIncrement_NRF_BIG_PPR_ATHR_INFO_ITGTmpList() {
		return "delete\r\n" + 
				"from (\r\n" + 
				" select 1\r\n" + 
				" from DATAMART.NRF_BIG_PPR_ATHR_INFO_ITG a\r\n" + 
				"where exists (  \r\n" + 
				" 	select 1\r\n" + 
				"    from DATAMART.NRF_BIG_PPR_ATHR_INFO_ITG_TMP b\r\n" + 
				"    where a.RSCHR_REG_NO = b.RSCHR_REG_NO\r\n" + 
				"    and a.MNG_NO = b.MNG_NO\r\n" + 
				"    and a.SEQ_NO = b.SEQ_NO\r\n" + 
				"    and a.REG_DTTM = b.REG_DTTM\r\n" + 
				"    )\r\n" + 
				"  )";
	}
}
