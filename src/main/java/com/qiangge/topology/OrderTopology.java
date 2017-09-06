package com.qiangge.topology;

import com.qiangge.bolts.OrderCounterBolt;
import com.qiangge.bolts.OrderGlobalCounterBolt;
import com.qiangge.bolts.OrderSplitBolt;
import com.qiangge.spout.SimulateOrderMessageQueueSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

/**
 * Created by zhazha on 2017/8/24.
 */
public class OrderTopology {
	public static  void main(String[] args) throws Exception{
		TopologyBuilder builder =  new TopologyBuilder();
		builder.setSpout("orderMessager",new SimulateOrderMessageQueueSpout(),1);
		builder.setBolt("split",new OrderSplitBolt(),3).shuffleGrouping("orderMessager");
		builder.setBolt("count",new OrderCounterBolt(),4).fieldsGrouping("split",new Fields("receiveCountry","payTime"));
		builder.setBolt("globalCount",new OrderGlobalCounterBolt(),1).shuffleGrouping("count");
		Config config = new Config();
		//config.setDebug(true);
		if(args!=null&&args.length>0){
			config.setNumWorkers(3);
			StormSubmitter.submitTopology(args[0],config,builder.createTopology());
		}else {
			LocalCluster cluster = new LocalCluster();
			config.setNumWorkers(3);
			cluster.submitTopology("test",config,builder.createTopology());
			//Utils.sleep(10000);
			//cluster.killTopology("test");
			//cluster.shutdown();
		}


	}
}
