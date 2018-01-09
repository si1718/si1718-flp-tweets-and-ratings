package data.streaming.utils2;

import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import data.streaming.utils.Utils;

public class AllWindowFunctionImpl
		implements org.apache.flink.streaming.api.functions.windowing.AllWindowFunction<String, String, TimeWindow> {

	
	
	@Override
	public void apply(TimeWindow arg0, Iterable<String> arg1, Collector<String> arg2) throws Exception {
		
		for(String i : arg1) {
			
				arg2.collect(i);
			
		}
		
	}

}
