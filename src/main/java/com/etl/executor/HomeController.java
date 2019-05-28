package com.etl.executor;

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
	        exe.readHadoop();
	        
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
	
}
