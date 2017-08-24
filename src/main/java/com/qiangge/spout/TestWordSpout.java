package com.qiangge.spout;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.Map;
import java.util.Random;

/**
 * Created by zhazha on 2017/8/24.
 */
public class TestWordSpout extends BaseRichSpout {

	SpoutOutputCollector collector;
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector){
		this.collector = collector;
	}

	public void nextTuple(){
		Utils.sleep(1000);
		final String[] words = new String[]{"nathan","mike","jackson","golda","bertels"};
		final Random rand = new Random();
		final String word = words[rand.nextInt(words.length)];
		collector.emit(new Values(word));
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer){
		declarer.declare(new Fields("words"));
	}
}
