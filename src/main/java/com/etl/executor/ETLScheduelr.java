package com.etl.executor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ETLScheduelr  {
	
    private static final Logger logger = LoggerFactory.getLogger(ETLScheduelr.class);
   
//    @Scheduled(cron="* 10 * * * *") 
//    @Scheduled(fixedDelay=1000*10) 
//    public void scheduleRun() {
// 
//    	ConfigurableApplicationContext ctx = new GenericXmlApplicationContext();
//        final ConfigurableEnvironment env = ctx.getEnvironment();
//        MutablePropertySources propertySources = env.getPropertySources();
//        
//        try {
//            propertySources.addLast(new ResourcePropertySource("classpath:db.properties"));
//        } catch (IOException e) {
//            // TODO Auto-generated catch block
//            e.printStackTrace();
//        }
//    	
//    	Calendar calendar = Calendar.getInstance();
// 
//    	SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//    	System.out.println("스케쥴러 실행중 === " + dateFormat);
//    	logger.info("스케줄 실행 : " + dateFormat.format(calendar.getTime()));
//    }
    
}
