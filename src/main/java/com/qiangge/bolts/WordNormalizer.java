package com.qiangge.bolts;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class WordNormalizer extends BaseRichBolt {
	private static final long serialVersionUID = 1L;
	private OutputCollector collector;

	public void cleanup() {
	}

	/**
	 * *bolt*从单词文件接收到文本行，并标准化它。
	 * 文本行会全部转化成小写，并切分它，从中得到所有单词。
	 */
	public void execute(Tuple input) {
		Long now = System.currentTimeMillis();
		if (now - startTime > 500l) {
			startTime = now;
			System.err.println("============================当前时间变为：" + startTime + "==================================");

			String sentence = input.getString(0);
			String[] words = sentence.split(" ");
			for (String word : words) {
				word = word.trim();
				if (!word.isEmpty()) {
					word = word.toLowerCase();
					//发布这个单词
					List a = new ArrayList();
					a.add(input);
					collector.emit(a, new Values(word));
				}
			}
		}
	}

	Long startTime = null;

	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;

		startTime = System.currentTimeMillis();

		System.err.println("==========================当前时间：" + startTime + "=========================================");

	}

	/**
	 * 这个*bolt*只会发布“word”域
	 */
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
}
