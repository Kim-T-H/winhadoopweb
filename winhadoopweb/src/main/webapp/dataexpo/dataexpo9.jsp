<%@ page language="java" contentType="text/html; charset=UTF-8"
    pageEncoding="UTF-8"%>
<%@ taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core" %>
<%@ taglib prefix="fn" uri="http://java.sun.com/jsp/jstl/functions" %>
<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<title>Grid Lines Display Settings</title>
<script src="https://ajax.googleapis.com/ajax/libs/jquery/3.4.1/jquery.min.js" ></script>
<script src="https://www.chartjs.org/dist/2.9.3/Chart.min.js"></script>
<style>
	canvas{
		-moz-user-select : none;
		-webkit-user-select : none;
		-ms-user-select : none;
	}
	.chart-container {
		width : 500px;
		margin-left : 40px;
		margin-right : 40px;
		margin-bottom : 40px;
	}
	.container {
		display : flex;
		flex-direction : row;
		flex-wrap : wrap;
		justify-content : center;
	}
</style>
</head>
<body>
	<h3>항공사별 출발/도착 정보, 운항(1000마일 기준) 분석</h3>
	<form action="${pageContext.request.contextPath}/CarrGridServlet" method="post" name="f">
		<select name="year">
			<c:forEach var="y" begin="1987" end="2008">
				<option <c:if test="${param.year==y}">selected</c:if>>${y}</option>
			</c:forEach>
		</select>&nbsp;&nbsp;&nbsp;
		<input type="radio" name="kbn" value="s" checked>정시&nbsp;&nbsp;
	 	<input type="radio" name="kbn" value="d" 
	 		<c:if test="${param.kbn=='d'}">checked</c:if>>지연&nbsp;&nbsp;
	 	<input type="radio" name="kbn" value="e" 
	 		<c:if test="${param.kbn=='e'}">checked</c:if>>조기&nbsp;&nbsp;
	 	<input type="submit" value="데이터분석">
	</form><br>
	<c:if test="${!empty file}">
		<div class="container"></div>
		<script>
			arrcolor = new Array("rgb(255,0,0,1)","rgb(0,255,0,1)","rgb(0,0,255,1)");
	 		var kbn = "${param.kbn=='s'?'정시':param.kbm=='d'?'지연':'조기'}";
			var label = new Array(kbn + "출발건수", kbn + "도착건수", "운항거리(1000마일)");
			window.chartColors = {
					red : 'rgb(255,99,132)',
					orange : 'rgb(255,159,64)',
					yellow : 'rgb(255,205,86)',
					green : 'rgb(75,192,192)',
					blue : 'rgb(54,162,235)',
					purple : 'rgb(153,102,255)',
					grey : 'rgb(201,203,207)'
			};
			function createConfig(gridLines, title, type) {
				return {
					type : type,
					data : {
						labels : [<c:forEach items="${list[0]}" var="m">"${m.key}",</c:forEach>],
						datasets : [
							<c:forEach items="${list}" var="map" varStatus="stat">
								{
									label : '${file}년' + label[${stat.index}],
									borderColor : [<c:forEach items="${map}" var="m">arrcolor[${stat.index}],</c:forEach>],
									backgroundColor : [<c:forEach items="${map}" var="m">arrcolor[${stat.index}],</c:forEach>],
									data : [<c:forEach items="${map}" var="m">"${m.value}",</c:forEach>],
									fill : false
								},
							</c:forEach>
						]
					},
					options : {
						responsive : true,
						title : {
							display : true,
							text : title
						},
						scales : {
							xAxes : [{
								fridLines : gridLines
							}],
							yAxes : [{
								gridLines : gridLines
							}]
						}
					}
				};
			}
			window.onload = function(){
				var container = document.querySelector('.container');
				[
					{
						title : '항공사별' + kbn + ' 현황',
						gridLines : {
							display : true
						},
						type : "bar"
					},
					{
						title : '항공사별' + kbn + ' 현황' ,
						gridLines : {
							display : true
						},
						type : "line"
					}
				].forEach(function(details) {
					var div = document.createElement('div');
					div.classList.add('chart-container');
					var canvas = document.createElement("canvas");
					div.appendChild(canvas);
					container.appendChild(div);
					var ctx = canvas.getContext('2d');
					var config = createConfig(details.gridLines, details.title, details.type);
					new Chart(ctx, config);
				});
			}
		</script>
	</c:if>
</body>
</html>