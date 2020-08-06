package dataexpo;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class CarrMultiTypeReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	
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
		outKey.set(columns[1]);
		if(columns[0].equals("D")) {	//지연정보
			int sum = 0;
			for(IntWritable v : values) {
				sum += v.get();
			}
			result.set(sum);
			mos.write("delayed", outKey, result);
		} else if (columns[0].equals("O")){	//정시정보
			int sum = 0;
			for(IntWritable v: values) {
				sum += v.get();
			}
			result.set(sum);
			mos.write("onTime", outKey, result);
		} else if (columns[0].equals("E")){	//조기정보
			int sum = 0;
			for(IntWritable v: values) {
				sum += v.get();
			}
			result.set(sum);
			mos.write("earlier", outKey, result);
		}else if (columns[0].equals("DI")){	//거리정보
			long sum = 0;
			for(IntWritable v: values) {
				sum += v.get();
			}
			result.set((int)(sum/1000));
			mos.write("distance", outKey, result);
		} 
	}
	@Override
	public void cleanup (Context context) throws IOException, InterruptedException {
		mos.close();
	}
}
