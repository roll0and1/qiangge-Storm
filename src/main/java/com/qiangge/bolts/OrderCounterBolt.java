package com.qiangge.bolts;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.Utils;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

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
		Date curDate = new Date();
		SimpleDateFormat dmf = new SimpleDateFormat("yyyy-MM-dd");
		curDateString = dmf.format(new Date());

	}

	@Override
	public void execute(Tuple tuple) {
		Integer num = 0;
		if (tuple.getString(1) != null && tuple.getString(1).indexOf(curDateString) != -1) {
			if (count.get(tuple.getString(0)) != null) {
				num = count.get(tuple.getString(0));
			}
			num++;
			count.put(tuple.getString(0), num);

		}
		Utils.sleep(3000);
		System.err.println("==========================收货国统计==========================");
		Set<String> orderSet = count.keySet();

		for (String country : orderSet) {
			System.err.println(country + "=================>" + count.get(country));
		}
		//this.collector.ack(tuple);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

	@Override
	public void cleanup() {

		System.err.println("==========================收货国统计==========================");
		Set<String> orderSet = count.keySet();

		for (String country : orderSet) {
			System.err.println(country + "=================>" + count.get(country));
		}
	}
}
