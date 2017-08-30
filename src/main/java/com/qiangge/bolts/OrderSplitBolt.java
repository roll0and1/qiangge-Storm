package com.qiangge.bolts;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * Created by zhazha on 2017/8/24.
 */
public class OrderSplitBolt extends BaseRichBolt {
	OutputCollector collector;

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple tuple) {
		String[] msg = tuple.getString(0).split(";");
		String receiveCountry = msg[0];
		String payTime = msg[1];
		this.collector.emit(new Values(receiveCountry, payTime));
		this.collector.ack(tuple);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("receiveCountry", "payTime"));
	}
}
