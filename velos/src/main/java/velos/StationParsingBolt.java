package velos;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.shade.org.json.simple.JSONObject;
import org.apache.storm.shade.org.json.simple.parser.JSONParser;
import org.apache.storm.shade.org.json.simple.parser.ParseException;

public class StationParsingBolt extends BaseRichBolt {
	private OutputCollector outputCollector;
	
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		outputCollector = collector;
	}

	@Override
	public void execute(Tuple input) {
		try {
			process(input);
		} catch (ParseException e) {
			e.printStackTrace();
			outputCollector.fail(input);
		}
	}
	
	public void process(Tuple input) throws ParseException {
		JSONParser jsonParser = new JSONParser();
		JSONObject obj = (JSONObject)jsonParser.parse(input.getStringByField("value"));
		String contract = (String)obj.get("contract_name");
		Long availableStands = (Long)obj.get("available_bike_stands");
		Long stationNumber = (Long)obj.get("number");
		
		outputCollector.emit(new Values(contract, stationNumber, availableStands));
		outputCollector.ack(input);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("city", "station_id", "available_stands"));
	}
}
