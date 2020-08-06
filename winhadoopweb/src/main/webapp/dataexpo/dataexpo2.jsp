<%@ page language="java" contentType="text/html; charset=UTF-8"
    pageEncoding="UTF-8"%>
<%@ taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core" %>
<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<title>항공사별 항공기 지연 데이터 분석</title>
<script src="https://ajax.googleapis.com/ajax/libs/jquery/3.4.1/jquery.min.js" ></script>
<script src="https://www.chartjs.org/dist/2.9.3/Chart.min.js"></script>
</head>
<body>
	<h3>항공사별 항공기 지연 데이터 분석 : 막대그래프</h3>
	<form action="${pageContext.request.contextPath}/CarrServlet" method="post" name="f">
		<select name="year">
			<c:forEach var="y" begin="1987" end="1988">
				<option <c:if test="${param.year == y}">selected</c:if>>${y}</option>
			</c:forEach>
		</select>
		<input type="radio" name="kbn" value="a" checked="checked">도착지연&nbsp;
		<input type="radio" name="kbn" value="d" <c:if test="${param.kbn=='d'}">checked="checked"</c:if>>출발지연&nbsp;<br>
		<input type="radio" name="graph" value="bar" checked="checked">막대그래프&nbsp;
		<input type="radio" name="graph" value="pie" <c:if test="${param.graph=='pie'}">checked="chedcked"</c:if>>파이그래프&nbsp;
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
			var randomColor = function(opacity){
				return "rgba(" + randomColorFactor() + "," + randomColorFactor() + "," + randomColorFactor() + "," + (opacity || "0.3") + ")";
			}
			rcolor = randomColor(1);
			var config = {
					type: '${param.graph}',
					data : {
						datasets : [{
							label : "${file}${(param.kbn=='a')?'도착':'출발'}지연정보",
							data : [<c:forEach items="${map}" var="m">"${m.value}",</c:forEach>],
							backgroundColor : [<c:forEach items="${map}" var="m">randomColor(1),</c:forEach>]
						}],
						labels : [<c:forEach items="${map}" var="m">"${m.key}",</c:forEach>]
					},
					options : {
						responsive : true
					}
			};
			window.onload = function() {
				var ctx = document.getElementById("chart").getContext("2d");
				console.log(ctx);
				new Chart(ctx,config);
			}
		</script>
	</c:if>
</body>
</html>