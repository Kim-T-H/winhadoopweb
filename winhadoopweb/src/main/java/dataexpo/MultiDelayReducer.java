package dataexpo;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class MultiDelayReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	private MultipleOutputs<Text, IntWritable> mos;
	private Text outKey = new Text();
	private IntWritable result = new IntWritable();
	
	@Override
	public void setup(Context context) {
		mos = new MultipleOutputs<Text, IntWritable>(context);
	}
	@Override
	public void reduce (Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		String[] columns = key.toString().split("-");
		outKey.set(columns[1] + "-" + columns[2]);
		if(columns[0].equals("D")) {
			int sum = 0;
			for(IntWritable v : values) {
				sum += v.get();
			}
			result.set(sum);;
			mos.write("departure", outKey, result);
		} else {
			int sum = 0;
			for(IntWritable v: values) {
				sum += v.get();
			}
			result.set(sum);;
			mos.write("arrival", outKey, result);
		}
	}
	@Override
	public void cleanup (Context context) throws IOException, InterruptedException {
		mos.close();
	}
}
