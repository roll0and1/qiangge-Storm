package com.qiangge.bolts;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by zhazha on 2017/8/24.
 */
public class OrderCounterBolt extends BaseRichBolt {
	OutputCollector collector;
	String curDateString = null;
	Map<String, Integer> count = new HashMap<String, Integer>();

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		//Date curDate = new Date();
		//SimpleDateFormat dmf = new SimpleDateFormat("yyyy-MM-dd");
		//curDateString = dmf.format(new Date());

	}

	Integer num = null;

	@Override
	public void execute(Tuple tuple) {
		//if (tuple.getString(1) != null && tuple.getString(1).indexOf(curDateString) != -1) {
		String receiveCountry = tuple.getString(0);
		String dateTime = tuple.getString(1);


		if (receiveCountry != null && dateTime != null) {
			num = count.get(receiveCountry + "_" + dateTime);
			if(num == null){
				num = 0;
			}
			num++;
			count.put(receiveCountry + "_" + dateTime, num);

			collector.emit(new Values(receiveCountry + "_" + dateTime, num));

		}

		//}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("receiveCountryDate", "count"));
	}

	@Override
	public void cleanup() {

	}
}
