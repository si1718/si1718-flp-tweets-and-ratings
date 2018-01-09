package data.streaming.utils;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import org.apache.flink.streaming.connectors.twitter.TwitterSource;

public class LoggingFactory {
	public static final String PROPERTIES_FILE = "resources/data.properties";

	public static Properties getTwitterCredentias() throws FileNotFoundException, IOException {
		Properties result = new Properties();

		Properties props = new Properties();
		props.load(new FileInputStream(PROPERTIES_FILE));

		result.setProperty(TwitterSource.CONSUMER_KEY, props.getProperty("TWITTER_CONSUMER_KEY").trim());
		result.setProperty(TwitterSource.CONSUMER_SECRET, props.getProperty("TWITTER_CONSUMER_SECRET").trim());
		result.setProperty(TwitterSource.TOKEN, props.getProperty("TWITTER_TOKEN").trim());
		result.setProperty(TwitterSource.TOKEN_SECRET, props.getProperty("TWITTER_TOKEN_SECRET").trim());
		return result;
	}

	public static Properties getCloudKarafkaCredentials() throws FileNotFoundException, IOException {
		Properties result = new Properties();

		
		Properties props = new Properties();
		props.load(new FileInputStream(PROPERTIES_FILE));

		String jaasTemplate = "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"%s\" password=\"%s\";";
		String jaasCfg = String.format(jaasTemplate, props.getProperty("CLOUDKARAFKA_USERNAME").trim(),
				props.getProperty("CLOUDKARAFKA_PASSWORD").trim());

		result.put("bootstrap.servers", props.getProperty("CLOUDKARAFKA_BROKERS").trim());
		result.put("group.id", "newer");
		result.put("enable.auto.commit", "true");
		result.put("auto.commit.interval.ms", "1000");
		result.put("auto.offset.reset", "earliest");
		result.put("session.timeout.ms", "30000");
		result.put("security.protocol", "SASL_SSL");
		result.put("sasl.mechanism", "SCRAM-SHA-256");
		result.put("sasl.jaas.config", jaasCfg);
		
		result.put("CLOUDKARAFKA_TOPIC", props.get("CLOUDKARAFKA_TOPIC"));
		
		return result;
	}
}
