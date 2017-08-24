package com.qiangge.topology;

import com.qiangge.bolts.ExclamationBolt;
import com.qiangge.spout.TestWordSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;

/**
 * Created by zhazha on 2017/8/24.
 */
public class ExclamationTopology {
	public static  void main(String[] args) throws Exception{
		TopologyBuilder builder =  new TopologyBuilder();
		builder.setSpout("word",new TestWordSpout(),10);
		builder.setBolt("exlamim1",new ExclamationBolt(),3).shuffleGrouping("word");
		builder.setBolt("exlamim2",new ExclamationBolt(),2).shuffleGrouping("exlamim1");
		Config config = new Config();
		config.setDebug(true);
		if(args!=null&&args.length>0){
			config.setNumWorkers(3);
			StormSubmitter.submitTopology(args[0],config,builder.createTopology());
		}else {
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("test",config,builder.createTopology());
			Utils.sleep(10000);
			cluster.killTopology("test");
			cluster.shutdown();
		}


	}
}
