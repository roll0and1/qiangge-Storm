package com.qiangge.bolts;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

public class WordCounter extends BaseRichBolt {

	private static final long serialVersionUID = 1L;
	Integer id;
	String name;
	Map<String, Integer> counters;
	private OutputCollector collector;

	/**
	 * 这个spout结束时（集群关闭的时候），我们会显示单词数量
	 */
	@Override
	public void cleanup() {
		System.err.println("-- 单词数 ["+name+"-"+id+"] --");
		for(Map.Entry<String, Integer> entry : counters.entrySet()){
			System.err.println(entry.getKey()+": "+entry.getValue());
		}
	}

	/**
	 * On create
	 */
	public void prepare(Map stormConf, TopologyContext context) {
		this.counters = new HashMap<String, Integer>();
		this.name = context.getThisComponentId();
		this.id = context.getThisTaskId();
	}


	/**
	 *  为每个单词计数
	 */
	@Override
	public void execute(Tuple input) {
		String str=input.getString(0);
		/**
		 * 如果单词尚不存在于map，我们就创建一个，如果已在，我们就为它加1
		 */
		if(!counters.containsKey(str)){
			counters.put(str,1);
		}else{
			Integer c = counters.get(str) + 1;
			counters.put(str,c);
		}
	}


	/**
	 * 初始化
	 */
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector){
		this.counters = new HashMap<String, Integer>();
		this.collector = collector;
		this.name = context.getThisComponentId();
		this.id = context.getThisTaskId();
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {}

	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
}
