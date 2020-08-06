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
<title>하둡을 이용한 파일 목록 보기</title>
</head>
<body>
<%
	Configuration conf = new Configuration();
	String path = "d:/ubuntu_share/dataexpo";
	Path root = new Path(path);
	FileSystem fs = FileSystem.get(conf);
%>
<h2>하웁을 이용한 파일 목록</h2>
<%
	//지정된 폴더의 하위 파일 목록 리턴
	RemoteIterator<LocatedFileStatus> flist = fs.listLocatedStatus(root);
	while(flist.hasNext()){
		LocatedFileStatus lfs = flist.next();
		if(lfs.isDirectory()) {	//폴더?
%>
			<a href="disp2.jsp?path=<%=lfs.getPath().getName()%>">d--<%=lfs.getPath().getName()%></a><br>
<%		} else {
%>
			<a href="disp3.jsp?file=<%=lfs.getPath().getName()%>&path=<%=path%>">---<%=lfs.getPath().getName()%></a><br>
<%
		}
	}
%>
</body>
</html>