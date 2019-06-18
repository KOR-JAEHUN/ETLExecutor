package com.etl.executor;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.springframework.scheduling.annotation.Scheduled;

public class ETLScheduler  {

//    @Scheduled(cron="0 7 15 * * *") 
//    public void scheduleRunByLake() {
//        long startTime = System.currentTimeMillis();
//
//        Executor exe = new Executor();
//		
//        	exe.copyTableOdsToLake();
//        
//        long endTime = System.currentTimeMillis();
//        long time = endTime - startTime;
//        System.out.println("실행시간 =============== " + time/1000.0 + "초");
//    }
    
	@Scheduled(cron="0 0 1 * * *") 
    public void scheduleRunByEtl() {
    	try {
			long startTime = System.currentTimeMillis();
			InetAddress local = InetAddress.getLocalHost();
			String ip = local.getHostAddress();
			
			Executor exe = new Executor();
			boolean success = exe.copyTableOdsToLake();
			if(success) {
				exe.etlStart();
				exe.createMasterTable();
				if("100.100.100.218".contains(ip)) {
					exe.copyHadoopLog();
					//exe.startMongo();
				}
			}
			
			long endTime = System.currentTimeMillis();
			long time = endTime - startTime;
			System.out.println("실행시간 =============== " + time/1000.0 + "초");
		} catch (UnknownHostException e) {
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
    }
    
}
