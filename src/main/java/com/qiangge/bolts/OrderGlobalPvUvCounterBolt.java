package com.qiangge.bolts;

import org.apache.commons.io.FileUtils;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.Utils;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Created by zhazha on 2017/8/24.
 */
public class OrderGlobalPvUvCounterBolt extends BaseRichBolt {
	OutputCollector collector;
	String curDateString = null;
	Map<String, Integer> counter = new HashMap<String, Integer>();

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		Date curDate = new Date();
		SimpleDateFormat dmf = new SimpleDateFormat("yyyy-MM-dd");
		curDateString = dmf.format(new Date());

	}

	@Override
	public void execute(Tuple tuple) {
		Integer pv = 0;
		Integer uv = 0;
		Integer num = 0;
		String receiveAndDateString = tuple.getString(0); // 日期
		String receiveCountry = receiveAndDateString.split("_")[0];
		String dateString = receiveAndDateString.split("_")[1];
		Integer count = tuple.getInteger(1);

		if (dateString != null && dateString.startsWith(curDateString) && dateString.compareTo(curDateString) > 0) { //跨天
			curDateString = dateString;
			counter.clear();
		}
		counter.put(receiveCountry, count);
		Utils.sleep(3000);
		System.err.println("==========================收货国统计==========================");

		Set<String> orderSet = counter.keySet();

		for (String country : orderSet) {
			Integer receiveCountryCount = counter.get(country);
			System.err.println(country + "=================>" + counter.get(country));

			if (receiveCountryCount != null) {
				pv = pv + counter.get(country);
				uv++;
			}
			try {
				FileUtils.write(new File("C:\\Users\\zhazha\\Desktop\\uv.txt"), "uv:=================>" + uv + "\r\n", true);
				FileUtils.write(new File("C:\\Users\\zhazha\\Desktop\\pv.txt"), "pv:=================>" + pv + "\r\n", true);
				System.err.println("======================uv:"+uv+";pv:"+pv+"=======================");
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		//this.collector.ack(tuple);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

	@Override
	public void cleanup() {

		/*System.err.println("==========================收货国统计==========================");
		Set<String> orderSet = count.keySet();

		for (String country : orderSet) {
			System.err.println(country + "=================>" + count.get(country));
		}*/
	}
}
