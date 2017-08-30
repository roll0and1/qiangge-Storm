package com.qiangge.spout;

import org.apache.commons.io.FileUtils;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Created by zhazha on 2017/8/24.
 */
public class SimulateOrderMessageQueueSpout extends BaseRichSpout {

	SpoutOutputCollector collector;
	String orderMessage = null;
	Queue<String> queue = new ConcurrentLinkedQueue<String>();

	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
		//Utils.sleep(1000);
		final String[] countries = {"英国", "美国", "德国", "比利时", "西班牙", "黎巴嫩"};
		final String payTime = "2017-08-30";
		for (int i = 0; i < 100; i++) {
			orderMessage = countries[new Random().nextInt(6)] + ";" + payTime;
			queue.add(orderMessage);
			System.err.println(orderMessage);
			try {
				FileUtils.write(new File("C:\\Users\\zhazha\\Desktop\\spout.txt"), orderMessage + "\r\n", true);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public void nextTuple() {
		//Utils.sleep(1000);
		if (queue != null && queue.size() > 0) {
			collector.emit(new Values(queue.poll()));
			Utils.sleep(500);
		}

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("orderMessage"));

	}

	public static void main(String[] args) {
		final String[] countries = {"英国", "美国", "德国", "比利时", "西班牙", "黎巴嫩"};
		for (int i = 0; i < 100; i++) {
			System.out.println(countries[new Random().nextInt(6)]);
		}

	}
}
