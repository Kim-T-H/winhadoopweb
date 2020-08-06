package dataexpo;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.FileAlreadyExistsException;
import java.util.Comparator;
import java.util.Map;
import java.util.TreeMap;

import javax.servlet.RequestDispatcher;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class MonDelayCntServlet extends HttpServlet {
	private static final long serialVersionUID = 1L;
    public MonDelayCntServlet() {
    	super();
    }
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		response.setContentType("text/html; charset=UTF-8");
		String year = request.getParameter("year");
		String kbn = request.getParameter("kbn");
		String input = "d:/ubuntushare/dataexpo/" + year + ".csv";
		String output = request.getSession().getServletContext().getRealPath("/") + "output/mondelay/" + year + "_" + kbn;
		Configuration conf = new Configuration();
		try {
			Job job = new Job(conf, "MonDelayCntServlet");
			job.setJarByClass(this.getClass());
			if(kbn.equals("a")) {
				job.setMapperClass(ArrivalDelayMapper.class);
			} else {
				job.setMapperClass(DepartureDelayMapper.class);
			}
			job.setReducerClass(DelayCountReducer.class);
			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			FileInputFormat.addInputPath(job, new Path(input));
			FileOutputFormat.setOutputPath(job, new Path(output));
			job.waitForCompletion(true);
		} catch (FileAlreadyExistsException e) {
			System.out.println("기존 파일 존재 : " + output);
		} catch (Exception e) {
			e.printStackTrace();
		}
		String file= "part-r-00000";
		request.setAttribute("file", year);
		Path outFile = new Path(output + "/" + file);
		FileSystem fs = FileSystem.get(conf);
		BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(outFile)));
//		Map<String, Integer> map = new TreeMap<String,Integer>((o1,o2)->Integer.parseInt(o1.split("-")[1])-Integer.parseInt(o2.split("-")[1]));
		Map<String,Integer> map = new TreeMap<String,Integer>(new Comparator<String>() {
			@Override
			public int compare(String o1, String o2) {
				return Integer.parseInt(o1.split("-")[1])-Integer.parseInt(o2.split("-")[1]);
			}
		});
		String line = null;
		while((line = br.readLine()) != null) {
			String[] v = line.split("\t");
			map.put(v[0].trim(), Integer.parseInt(v[1].trim()));
		}
		request.setAttribute("map", map);
		String view = request.getParameter("view");
		if(view == null) view = "1";
		RequestDispatcher dispatcher = request.getRequestDispatcher("/dataexpo/dataexpo" + view + ".jsp");
		dispatcher.forward(request, response);
	} 
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		doGet(request,response);
	}
}
