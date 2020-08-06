<%@ page language="java" contentType="text/html; charset=UTF-8"
    pageEncoding="UTF-8"%>
<%@ taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core" %>
<%@ taglib prefix="fn" uri="http://java.sun.com/jsp/jstl/functions" %>
<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<title>월별로 도착/출발 지연 북석(정시/지연/초기) : 선그래프 작성하기</title>
<script src="https://ajax.googleapis.com/ajax/libs/jquery/3.4.1/jquery.min.js" ></script>
<script src="https://www.chartjs.org/dist/2.9.3/Chart.min.js"></script>
</head>
<body>
	<h3>월별 출발/도착 정보, 운항(1000마일 기준) 분석 : 선그래프</h3>
	<form action="${pageContext.request.contextPath}/MonMultiServlet2" method="post" name="f">
		<input type="hidden" name="view" value="7">
		<select name="year">
			<c:forEach var="y" begin="1987" end="1988">
				<option <c:if test="${param.year==y}">selected</c:if>>${y}</option>
			</c:forEach>
		</select>&nbsp;&nbsp;&nbsp;
	 	<input type="radio" name="kbn" value="s" checked>정시&nbsp;&nbsp;
	 	<input type="radio" name="kbn" value="d" 
	 		<c:if test="${param.kbn=='d'}">checked</c:if>>지연&nbsp;&nbsp;
	 	<input type="radio" name="kbn" value="e" 
	 		<c:if test="${param.kbn=='e'}">checked</c:if>>조기&nbsp;&nbsp;
	 	<input type="submit" value="데이터분석">
	</form>
	<c:if test="${!empty file}">
	 	<div id="canvas-holder" style="width:70%;height:300px;">
	 		<canvas id="chart-area" width="90%" height="90%"></canvas>
	 	</div>
	 	<script>
	 		arrcolor = new Array("rgb(255,0,0,1)","rgb(0,255,0,1)","rgb(0,0,255,1)");
	 		var kbn = "${param.kbn=='s'?'정시':param.kbm=='d'?'지연':'조기'}";
			var label = new Array(kbn + "출발건수", kbn + "도착건수", "운항거리(1000마일)");
			var config = {
					type : 'line',	//선그래프로 그래프 설정
					data : {
						datasets : [
							<c:forEach items="${list}" var="map" varStatus="stat"> 
								{
									label : '${file}년' + label[${stat.index}],
									data : [<c:forEach items="${map}" var="m">"${m.value}",</c:forEach>],
									borderColor : [<c:forEach items="${map}" var="m">arrcolor[${stat.index}],</c:forEach>],
									fill : false
								},
							</c:forEach>
						],
						labels : [<c:forEach items="${list[0]}" var="m">"${fn:split(m.key,'-')[1]}월",</c:forEach>]
					},.
					
					options : {
						responsive : true,
						hover : {
							mode : 'nearest'
						},
						title : {
							display : true,
							text : "${file}년도 월별" + kbn + "현황"
						},
						scales : {
							xAxes : [{
								display : true,
								scaleLabel : {
									display : true,
									labelString : "${file}년도"
								}
							}],
							yAxes : [{
								display : true,
								scaleLabel : {
									display : true,
									labelString : label[0] + "/" + label[1] + "/" + label[2]
								}
							}]
						}
					}
			};
			window.onload = function(){
				var ctx = document.getElementById("chart-area").getContext("2d");
				new Chart(ctx, config);
			}
	 	</script>
	 </c:if>
</body>
</html>