package analytics;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

public class UserVisitCountBolt extends BaseRichBolt {
	private OutputCollector outputCollector;
	private HashMap<Integer, Integer> userVisitCounts;

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		userVisitCounts = new HashMap<Integer, Integer>();
		outputCollector = collector;
	}

	@Override
	public void execute(Tuple input) {
		Integer userId = input.getIntegerByField("userId");
		userVisitCounts.put(userId, userVisitCounts.getOrDefault(userId, 0) + 1);
		
		if(ThreadLocalRandom.current().nextInt(10) == 0) {
			outputCollector.fail(input);
		}
		else {
			outputCollector.ack(input);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

}
