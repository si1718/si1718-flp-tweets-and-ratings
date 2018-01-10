package data.streaming.utils;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

import data.streaming.dto.TweetDTO;

public class Utils {

	public static final String[] TAGNAMES = extractKeywordsFromDB();
	private static final ObjectMapper mapper = new ObjectMapper();

	private static List<org.bson.Document> tweetList = new ArrayList<>();

	public static TweetDTO createTweetDTO(String json) {
		TweetDTO result = null;

		try {
			result = mapper.readValue(json, TweetDTO.class);
		} catch (IOException e) {

		}
		if (result != null && (result.getCreatedAt() == null))
			result = null;
		// System.out.println("Created");
		return result;
	}

	public static Boolean isValid(String x) {
		Boolean result = true;

		if (x == null || createTweetDTO(x) == null)
			result = false;
		// System.out.println("Filtered:" + result);
		return result;
	}

	public static String[] extractKeywordsFromDB() {

		// DB access used in the microservice secure section
		MongoClientURI uri = new MongoClientURI(
				"mongodb://admin:passwordCurro@ds129386.mlab.com:29386/si1718-flp-grants-secure");

		// DB access used in the microservice basic section
		// MongoClientURI uri = new
		// MongoClientURI("mongodb://curro:curro@ds149855.mlab.com:49855/si1718-flp-grants");
		
		MongoClient client = new MongoClient(uri);

		MongoDatabase db = client.getDatabase(uri.getDatabase());
		MongoCollection<org.bson.Document> grantsCollection = db.getCollection("grants");

		MongoCursor<String> keywordSet = grantsCollection.distinct("keywords", String.class).iterator();

		Set<String> tags = new HashSet<String>();

		while (keywordSet.hasNext()) {
			String keyword = keywordSet.next();

			/*
			 * if(Character.isUpperCase(keyword.charAt(0)) && keyword.length() >3 &&
			 * !Character.isUpperCase(keyword.charAt(1))) { String keywordProccessed =
			 * keyword.replace(",", ""); keywordProccessed = keywordProccessed.replace(":",
			 * ""); keywordProccessed = keywordProccessed.replace(".", "");
			 * totalKeywordSet.add(keywordProccessed);
			 * System.out.println(keywordProccessed); }
			 */
			keyword = keyword.replaceAll("\"", "").replaceAll("\\?", "").replaceAll("\\)", "");
			if (keyword.contains("/")) {
				for (String k : keyword.split("/")) {
					System.out.println(k);
					tags.add(k);
				}
			} else {
				System.out.println(keyword);
				tags.add(keyword);
			}
		}
		// Keyword array generation

		String[] tagNames = new String[tags.size()];
		int contador = 0;
		for (String s : tags) {
			if (contador < tagNames.length)
				tagNames[contador] = s;
			contador = contador + 1;
		}

		return tagNames;
	}

	public static TweetDTO insertInBD(TweetDTO t) throws ParseException {
		MongoClientURI uri = new MongoClientURI(
				"mongodb://admin:passwordCurro@ds129386.mlab.com:29386/si1718-flp-grants-secure");
		// MongoClientURI uri = new
		// MongoClientURI("mongodb://curro:curro@ds149855.mlab.com:49855/si1718-flp-grants");
		MongoClient client = new MongoClient(uri);
		MongoDatabase db = client.getDatabase(uri.getDatabase());

		// Get tweet DB

		MongoCollection<org.bson.Document> batch = db.getCollection("tweets");
		System.out.println(t.getText());

		SimpleDateFormat format = new SimpleDateFormat("EEE MMM dd HH:mm:ss ZZZZZ yyyy", Locale.ENGLISH);
		format.setLenient(true);
		Date dateS = format.parse(t.getCreatedAt());

		org.bson.Document userData = new org.bson.Document().append("idStr", t.getUser().getIdStr())
				.append("name", t.getUser().getName()).append("screenName", t.getUser().getScreenName())
				.append("friends", t.getUser().getFriends()).append("followers", t.getUser().getFollowers());
		org.bson.Document tweet = new org.bson.Document().append("creationDateLong", dateS.getTime())
				.append("creationDate", t.getCreatedAt()).append("language", t.getLanguage())
				.append("text", t.getText()).append("userData", userData);

		batch.insertOne(tweet);

		client.close();
		System.out.println("Cantidad de tweets acumulados: " + tweetList.size());
		return t;
	}
}
