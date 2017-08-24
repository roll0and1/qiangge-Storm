package com.qiangge.topology;

import com.qiangge.bolts.WordCounter;
import com.qiangge.bolts.WordNormalizer;
import com.qiangge.spout.WordReader;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;


public class TopologyMain {

	public static void main(String[] args) throws InterruptedException {

		/*TransactionalTopologyBuilder
		TweetsTransactionalSpoutCoordinator*/


		//Topology definition
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("word-reader", new WordReader());
		builder.setBolt("word-normalizer", new WordNormalizer())
				.shuffleGrouping("word-reader");
		builder.setBolt("word-counter", new WordCounter())
				.fieldsGrouping("word-normalizer", new Fields("word"));

		//Configuration
		Config conf = new Config();
//		conf.put("wordsFile", args[0]);
		conf.put("wordsFile", "D:\\ideaProjects\\qiangge-storm\\src\\main\\java\\words.txt");
//		conf.put("wordsFile", "words.txt");
		conf.setDebug(false);
		//Topology run
		conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("Getting-Started-Toplogie", conf, builder.createTopology());
		Thread.sleep(1000);
		cluster.shutdown();


		/*try {
			FileReader aFileReader= new FileReader("D:\\workspce-riskcontrol\\stromDemo\\src\\main\\java\\words.txt");
			System.out.println(aFileReader);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}*/
	}
}
