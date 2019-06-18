package com.etl.executor;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Locale;

import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

/**
 * Handles requests for the application home page.
 */
@Controller
public class HomeController {
	
	@RequestMapping(value = "/executor", method = RequestMethod.GET)
	public String home(Locale locale, Model model) {
		
		return "home";
	}
	
	@RequestMapping(value = "/executorStart", method = RequestMethod.GET)
	@ResponseBody
	public Boolean executor() {
		Boolean flag = new Boolean(true);
		try {
			System.out.println("적재 버튼 실행");
			
	        long startTime = System.currentTimeMillis();
	
	        Executor exe = new Executor();
	        exe.etlStart();
	        
	        long endTime = System.currentTimeMillis();
	        long time = endTime - startTime;
	        System.out.println("실행시간 =============== " + time/1000.0 + "초");
		} catch (Exception e) {
			System.out.println(e.getMessage());
			flag = new Boolean(false);
		}
		return flag;
	}
	
	@RequestMapping(value = "/copyLake", method = RequestMethod.GET)
	@ResponseBody
	public Boolean copyLake() {
		Boolean flag = new Boolean(true);
		try {
			System.out.println("Lake COPY 버튼 실행");
			
			long startTime = System.currentTimeMillis();
			
			Executor exe = new Executor();
			exe.copyTableOdsToLake();
			
			long endTime = System.currentTimeMillis();
			long time = endTime - startTime;
			System.out.println("실행시간 =============== " + time/1000.0 + "초");
		} catch (Exception e) {
			System.out.println(e.getMessage());
			flag = new Boolean(false);
		}
		
		return flag;
	}
	
	@RequestMapping(value = "/executeCrawl", method = RequestMethod.GET)
	@ResponseBody
	public Boolean executeCrawl() {
		Boolean flag = new Boolean(true);
		try {
			System.out.println("크롤링 실행");
			
			long startTime = System.currentTimeMillis();
			
//			exec("java -jar /app/crawl/Crawling.jar");
			exec("/app/crawl/crawler.sh");
//			System.out.println(result);
			
			long endTime = System.currentTimeMillis();
			long time = endTime - startTime;
			System.out.println("실행시간 =============== " + time/1000.0 + "초");
		} catch (Exception e) {
			System.out.println(e.getMessage());
			flag = new Boolean(false);
		}
		
		return flag;
	}
	
	public static String exec(String command){
        String result = "";
        Runtime rt = Runtime.getRuntime();
        Process p = null;
        StringBuffer sb = new StringBuffer();
        try{
            p=rt.exec(command);
            BufferedReader in = new BufferedReader(new InputStreamReader(p.getInputStream()));
            String cl = null;
            while((cl=in.readLine())!=null){
                sb.append(cl);
                System.out.println(cl.toString());
            }
            result = sb.toString();
            in.close();
        }catch(IOException e){
            e.printStackTrace();
            return "";
        }
        return result;
    }

	
}
