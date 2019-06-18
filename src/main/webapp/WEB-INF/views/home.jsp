<%@ page language="java" contentType="text/html; charset=UTF-8"
    pageEncoding="UTF-8"%>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
	<meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
	<title>Home</title>
</head>
<script src="resources/js/jquery-3.3.1.min.js"></script>
<script type="text/javascript">
	function start(){
		$("#startBtn").attr("disabled", true);
		$.ajax({
	   		url: "<%=request.getContextPath() %>/executorStart",
	   		type: "get",
	   		dataType: "json",
	   		contentType: "application/json",
	   		success: function(data){
	   			if(data){
					$("#startBtn").attr("disabled", false);
	   				alert("적재 완료");
	   			}
	   		},
	   		error: function(e){
	 			alert("적재 실패");
				$("#startBtn").attr("disabled", false);
	   		}
	   	});
	}
	
	function copy(){
		$("#copyBtn").attr("disabled", true);
		$.ajax({
	   		url: "<%=request.getContextPath() %>/copyLake",
	   		type: "get",
	   		dataType: "json",
	   		contentType: "application/json",
	   		success: function(data){
	   			if(data){
					$("#copyBtn").attr("disabled", false);
	   				alert("복제 완료");
	   			}
	   		},
	   		error: function(e){
				$("#copyBtn").attr("disabled", false);
	 			alert("복제 실패");
	   		}
	   	});
	}
	
	function executeCrawl(){
		if(!confirm("실행하시겠습니까?")) {
			return false;
		}
		
		$("#crawlBtn").attr("disabled", true);
		$.ajax({
	   		url: "<%=request.getContextPath() %>/executeCrawl",
	   		type: "get",
	   		dataType: "json",
	   		contentType: "application/json",
	   		success: function(data){
	   			if(data){
					$("#crawlBtn").attr("disabled", false);
	   				alert("크롤링 완료");
	   			}
	   		},
	   		error: function(e){
				$("#crawlBtn").attr("disabled", false);
	 			alert("크롤링 실패");
	   		}
	   	});
	}
</script>
<body>
<!-- 	<input type="button" id="startBtn" value="적재 실행" onclick="start();" /> -->
<!-- 	<input type="button" id="copyBtn" value="Lake 복제 실행" onclick="copy();" /> -->
	<input type="button" id="crawlBtn" value="크롤링 실행" onclick="executeCrawl();" />
</body>
</html>