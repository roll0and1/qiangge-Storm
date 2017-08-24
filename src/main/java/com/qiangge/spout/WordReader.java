package com.qiangge.spout;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Map;

public class WordReader extends BaseRichSpout {

	/**
	 *
	 */
	private static final long serialVersionUID = 1L;
	private SpoutOutputCollector collector;
	private FileReader fileReader;
	private boolean completed = true;

	/**
	 * The only thing that the methods will do It is emit each file line
	 */
	public void nextTuple() {
		/**
		 * The nextuple it is called forever, so if we have been readed the file
		 * we will wait and then return
		 */
		System.out.println("=========================begin============================");
		if (completed) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				// Do nothing
			}
			return;
		}
		String str;
		// Open the reader
		BufferedReader reader = new BufferedReader(fileReader);
		try {

			// Read all lines
			while ((str = reader.readLine()) != null) {

				/**
				 * By each line emmit a new value with the line as a their
				 */
				this.collector.emit(new Values(str), str);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} /*finally {
			completed = true;
		}*/
	}

	/**
	 * We will create the file and get the collector object
	 */
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		try {

			this.fileReader = new FileReader(conf.get("wordsFile").toString());
		} catch (FileNotFoundException e) {
			throw new RuntimeException("Error reading file [" + conf.get("wordsFile") + "]");
		}
		this.collector = collector;
	}

	/**
	 * Declare the output field "word"
	 */
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("line"));
	}


}
