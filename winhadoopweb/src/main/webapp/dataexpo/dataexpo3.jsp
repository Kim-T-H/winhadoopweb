<%@ page language="java" contentType="text/html; charset=UTF-8"
    pageEncoding="UTF-8"%>
<%@ taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core" %>
<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<title>월별 도착/출발 지연 분석 : 막대그래프</title>
<script src="https://ajax.googleapis.com/ajax/libs/jquery/3.4.1/jquery.min.js" ></script>
<script src="https://www.chartjs.org/dist/2.9.3/Chart.min.js"></script>
</head>
<body>
	<h3>월별 출발/도착 지연 분석</h3>
	<form action="${pageContext.request.contextPath}/MonMultiServlet" method="post" name="f">
		<select name="year">
			<c:forEach var="y" begin="1987" end="1988">
				<option <c:if test="${param.year==y}">selected</c:if>>${y}</option>
			</c:forEach>
		</select>
		<input type="submit" value="분석">
	</form>
	<c:if test="${!empty file}">
		<div id="canvas-holder" style="width:50%; height:300px;">
			<canvas id="chart" width="100%" height="100%"></canvas>
		</div>
		<script>
			var randomColorFactor = function(){
				return Math.round(Math.random() * 255);
			}
			var randomColor = function(op){
				return "rgba(" + randomColorFactor() + "," + randomColorFactor() + "," + randomColorFactor() + "," + (op || ".3") + ")";
			}
			rcolor = randomColor(1);
			rcolor2 = randomColor(0.5);
			var config = {
					type : "bar",
					data : {
						datasets : [{
							label : "${file}년 출발지연건수",
							data : [<c:forEach items="${map1}" var="m">"${m.value}",</c:forEach>],
							backgroundColor : [<c:forEach items="${map1}" var="m">rcolor,</c:forEach>]
						},{
							label : "${file}년 도착지연건수",
							data : [<c:forEach items="${map2}" var="m">"${m.value}",</c:forEach>],
							backgroundColor : [<c:forEach items="${map2}" var="m">rcolor2,</c:forEach>]
						}],
						labels : [<c:forEach items="${map1}" var="m">"${m.key}월",</c:forEach>]
					},
					options : {
						response : true
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