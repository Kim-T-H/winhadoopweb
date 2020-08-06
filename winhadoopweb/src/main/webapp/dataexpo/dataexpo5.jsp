<%@ page language="java" contentType="text/html; charset=UTF-8"
    pageEncoding="UTF-8"%>
<%@ taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core" %>
<%@ taglib prefix="fn" uri="http://java.sun.com/jsp/jstl/functions" %>
<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<title>월별로 도착/출발지연 분석(정시/지연/초기), 운항거리분석 : 막대그래프 작성하기</title>
<script src="https://ajax.googleapis.com/ajax/libs/jquery/3.4.1/jquery.min.js" ></script>
<script src="https://www.chartjs.org/dist/2.9.3/Chart.min.js"></script>
</head>
<body>
	<h3>월별 출발/도착 정보 : 막대그래프</h3>
	 <form action="${pageContext.request.contextPath}/MonMultiServlet2" method="post" name="f">
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
	 		<canvas id="chart" width="100%" height="100%"></canvas>
	 	</div>
	 	<script>
		 	var randomColorFactor = function(){
				return Math.round(Math.random() * 255);
			}
			var randomColor = function(op){
				return "rgba(" + randomColorFactor() + "," + randomColorFactor() + "," + randomColorFactor() + "," + (op || ".3") + ")";
			}
			arrcolor = new Array("rgb(255,0,0)","rgb(0,255,0)","rgb(0,0,255)");
			var kbn = "${param.kbn=='s'?'정시':param.kbm=='d'?'지연':'조기'}";
			var label = new Array(kbn + "출발건수", kbn + "도착건수", "운항거리(1000마일)");
			var config = {
					type : 'bar',
					data : {
						datasets : [
							<c:forEach items="${list}" var="map" varStatus="stat"> 
								{
									label : '${file}년' + label[${stat.index}],
									data : [<c:forEach items="${map}" var="m">"${m.value}",</c:forEach>],
									backgroundColor : [<c:forEach items="${map}" var="m">arrcolor[${stat.index}],</c:forEach>]
								},
							</c:forEach>
						],
						labels : [<c:forEach items="${list[0]}" var="m">"${fn:split(m.key,'-')[1]}월",</c:forEach>]
					},
					options : {
						responsive : true
					}
			};
			window.onload = function(){
				var ctx = document.getElementById("chart").getContext("2d");
				new Chart(ctx, config);
			}
	 	</script>
	 </c:if>
</body>
</html>