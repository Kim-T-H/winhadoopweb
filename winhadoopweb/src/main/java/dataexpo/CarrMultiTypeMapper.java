package dataexpo;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CarrMultiTypeMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	private String workType;
	private final static IntWritable ONE = new IntWritable(1);
	private final static IntWritable DISTANCE = new IntWritable(1);
	private Text outkey = new Text();
	@Override
	protected void setup(Context context) {
		workType = context.getConfiguration().get("workType");
	}
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		Airline al = new Airline(value);
		//운항거리 설정
		if(al.isDistanceAvailable() && al.getDistance() > 0) {
			outkey.set("DI-" + al.getUniqueCarrier());
			DISTANCE.set(al.getDistance());	//운항거리 설정
			context.write(outkey, DISTANCE);
		}
		switch (workType) {
			case "d" : //출발 정보
				if(al.isDepartureDelayAvailable() && al.getDepartureDelayTime() > 0) {	//지연
					outkey.set("D-" + al.getUniqueCarrier());
					context.write(outkey, ONE);
				}
				if(al.isDepartureDelayAvailable() && al.getDepartureDelayTime() == 0) {
					outkey.set("O-" + al.getUniqueCarrier());
					context.write(outkey, ONE);
				}
				if(al.isDepartureDelayAvailable() && al.getDepartureDelayTime() < 0) {	//조기
					outkey.set("E-" + al.getUniqueCarrier());
					context.write(outkey, ONE);
				}
				break;
			case "a" :	//도착 정보
				if(al.isArriveDelayAvailable() && al.getArriveDelayTime() > 0) {	//지연
					outkey.set("D-" + al.getUniqueCarrier());
					context.write(outkey, ONE);
				}
				if(al.isArriveDelayAvailable() && al.getArriveDelayTime() == 0) {
					outkey.set("O-" + al.getUniqueCarrier());
					context.write(outkey, ONE);
				}
				if(al.isArriveDelayAvailable() && al.getArriveDelayTime() < 0) {	//조기
					outkey.set("E-" + al.getUniqueCarrier());
					context.write(outkey, ONE);
				}
				break;
		}
	}
}