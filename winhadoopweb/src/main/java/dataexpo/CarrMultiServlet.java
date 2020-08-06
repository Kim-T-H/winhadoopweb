package dataexpo;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.TreeMap;

import javax.servlet.RequestDispatcher;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileAlreadyExistsException;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class CarrMultiServlet extends HttpServlet {
	private static final long serialVersionUID = 1L;
    public CarrMultiServlet() {
        super();
    }

	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		response.setContentType("text/html; charset=UTF-8");
		String year = request.getParameter("year");
		String input = "D:/ubuntushare/dataexpo/" + year + ".csv";
		String output = request.getSession().getServletContext().getRealPath("/") + "/carrmultiout" + year;
		Configuration conf = new Configuration();
		try {
			Job job = new Job(conf, "CarrMultiServlet");
			FileInputFormat.addInputPath(job, new Path(input));
			FileOutputFormat.setOutputPath(job, new Path(output));
			job.setJarByClass(this.getClass());
			job.setMapperClass(CarrMultiDelayMapper.class);
			job.setReducerClass(CarrMultiDelayReducer.class);
			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			MultipleOutputs.addNamedOutput(job, "departure", TextOutputFormat.class, Text.class, IntWritable.class);
			MultipleOutputs.addNamedOutput(job, "arrival", TextOutputFormat.class, Text.class, IntWritable.class);
			job.waitForCompletion(true);
		} catch (FileAlreadyExistsException e) {
			System.out.println("기존 파일 존재 : " + output);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		String departure ="departure-r-00000";
		String arrival ="arrival-r-00000";
		request.setAttribute("file", year);
		Path outD = new Path(output + "/" + departure);
		Path outA = new Path(output + "/" + arrival);
		FileSystem fs = FileSystem.get(conf);
		BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(outD)));
		Map<String, Integer> map1 = new TreeMap<String,Integer>();
		String line = null;
		while((line = br.readLine()) != null) {
			String[] v = line.split("\t");
			int cnt = Integer.parseInt(v[1].trim());
			map1.put(v[0].trim(), cnt);
		}
		request.setAttribute("map1", map1);
		
		br = new BufferedReader(new InputStreamReader(fs.open(outA)));
		Map<String, Integer> map2 = new TreeMap<String,Integer>();
		line = null;
		while((line = br.readLine()) != null) {
			String[] v = line.split("\t");
			int cnt = Integer.parseInt(v[1].trim());
			map2.put(v[0].trim(), cnt);
		}
		request.setAttribute("map2", map2);
		String jsp = "/dataexpo/dataexpo4.jsp";
		RequestDispatcher dispatcher = request.getRequestDispatcher(jsp);
		dispatcher.forward(request, response);
	}
	
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		doGet(request, response);
	}

}
