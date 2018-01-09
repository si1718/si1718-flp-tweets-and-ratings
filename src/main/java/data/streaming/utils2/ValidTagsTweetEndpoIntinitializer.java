package data.streaming.utils2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.flink.streaming.connectors.twitter.TwitterSource.EndpointInitializer;

import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.endpoint.StreamingEndpoint;

public class ValidTagsTweetEndpoIntinitializer implements EndpointInitializer, Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -3172979177938307047L;
	
	private String[] tagNames;
	
	
	public ValidTagsTweetEndpoIntinitializer(String[] tagNames) {
		super();
		this.tagNames = tagNames;
	}


	public StreamingEndpoint createEndpoint() {

		List<String> tags = new ArrayList<>();
		tags.addAll(Arrays.asList(tagNames));

		StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
		endpoint.trackTerms(tags);
		return endpoint;

	}

}
