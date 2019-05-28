package com.etl.executor;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.springframework.scheduling.annotation.Scheduled;

public class ETLScheduelr  {
	
    @Scheduled(cron="0 0 1 * * *") 
    public void scheduleRunByLake() {
        long startTime = System.currentTimeMillis();

        Executor exe = new Executor();
        InetAddress local;
        String ip = null;
		try {
			local = InetAddress.getLocalHost();
			ip = local.getHostAddress();
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
        if("100.100.100.218".contains(ip)) {
        	exe.copyTableOdsToLake();
        }
        
        long endTime = System.currentTimeMillis();
        long time = endTime - startTime;
        System.out.println("실행시간 =============== " + time/1000.0 + "초");
    }
    
    @Scheduled(cron="0 20 1 * * *") 
    public void scheduleRunByEtl() {
    	long startTime = System.currentTimeMillis();
    	
    	Executor exe = new Executor();
    	exe.readHadoop();
    	
    	long endTime = System.currentTimeMillis();
    	long time = endTime - startTime;
    	System.out.println("실행시간 =============== " + time/1000.0 + "초");
    }
    
}
