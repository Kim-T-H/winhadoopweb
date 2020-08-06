<%@page import="org.apache.hadoop.fs.LocatedFileStatus"%>
<%@page import="org.apache.hadoop.fs.RemoteIterator"%>
<%@page import="org.apache.hadoop.fs.FileSystem"%>
<%@page import="org.apache.hadoop.fs.Path"%>
<%@page import="org.apache.hadoop.conf.Configuration"%>
<%@ page language="java" contentType="text/html; charset=UTF-8"
    pageEncoding="UTF-8"%>
<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<title>Insert title here</title>
</head>
<body>
<%
	Configuration conf = new Configuration();
	String root ="d:/ubuntu_share/dataexpo/";	
	String path = request.getParameter("path");
	Path dir = new Path(root + path);
	FileSystem fs = FileSystem.get(conf);
%>
<h2>하둡 서버 파일 목록</h2>
<%
	RemoteIterator<LocatedFileStatus> flist = fs.listLocatedStatus(dir);
	while(flist.hasNext()){
		LocatedFileStatus lfs = flist.next();
		if(lfs.isDirectory()){
%>			
			<a href="disp2.jsp?path=<%=lfs.getPath().getName()%>">d--<%=lfs.getPath().getName()%></a><br>
<%		} else {
%>
			<a href="disp3.jsp?file=<%=lfs.getPath().getName()%>&path=<%=root+path%>">---<%=lfs.getPath().getName()%></a><br>
<%
		}
	}
%>
</body>
</html>