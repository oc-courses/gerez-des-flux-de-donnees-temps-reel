package analytics;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class VisitCountBolt extends BaseRichBolt {
	private OutputCollector outputCollector;
	private Integer totalVisitCount;
	
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		totalVisitCount = 0;
		outputCollector = collector;
	}

	@Override
	public void execute(Tuple input) {
		String url = input.getStringByField("url");
		Integer userId = input.getIntegerByField("userId");
		totalVisitCount += 1;
		
		outputCollector.emit(input, new Values(url, userId));
		outputCollector.ack(input);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("url", "userId"));
	}
}
