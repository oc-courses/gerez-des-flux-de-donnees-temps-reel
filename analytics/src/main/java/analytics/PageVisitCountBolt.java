package analytics;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

public class PageVisitCountBolt extends BaseRichBolt {
	private OutputCollector outputCollector;
	private HashMap<String, Integer> pageVisitCounts;

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		pageVisitCounts = new HashMap<String, Integer>();
		outputCollector = collector;
	}

	@Override
	public void execute(Tuple input) {
		String url = input.getStringByField("url");
		pageVisitCounts.put(url, pageVisitCounts.getOrDefault(url, 0) + 1);
		outputCollector.ack(input);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}
}
