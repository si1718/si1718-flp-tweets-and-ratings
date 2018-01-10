package data.streaming.test;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.bson.Document;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.FindOneAndReplaceOptions;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.result.UpdateResult;

public class KeywordStatsByTweetsAndDatesGeneration {
	
	/**
	 * 
	 * This process extract information from tweets stored in a database. It detects the number of matches for each keyword
	 * used in a Grant Database 
	 */
	public static void main(String... args) throws Exception {
		
		
		// DB access used in the microservice secure section
		MongoClientURI uri = new MongoClientURI("mongodb://admin:passwordCurro@ds129386.mlab.com:29386/si1718-flp-grants-secure");
		
		// DB access used in the microservice basic section
		//MongoClientURI uri = new MongoClientURI("mongodb://curro:curro@ds149855.mlab.com:49855/si1718-flp-grants");
		
		MongoClient client = new MongoClient(uri);
		MongoDatabase db = client.getDatabase(uri.getDatabase());
		
		//Tweets date format:
		SimpleDateFormat format = new SimpleDateFormat("EEE MMM dd HH:mm:ss ZZZZZ yyyy",Locale.ENGLISH);
		format.setLenient(true);
		
		// Date format for statistics
		SimpleDateFormat statisticsFormat = new SimpleDateFormat("yyyy-MM-dd");
		
		// Retrieving mongoDB collections
	
		MongoCollection<org.bson.Document> tweetsCollection = db.getCollection("tweets");
		MongoCollection<org.bson.Document> grantsCollection = db.getCollection("grants");
		MongoCollection<org.bson.Document> grantStatisticsCollection = db.getCollection("grantStatistics");
		
		MongoCursor<String> keywordSet = grantsCollection.distinct("keywords", String.class).iterator();
		
		
		Set<String> totalKeywordSet = new HashSet<String>();
		
		while(keywordSet.hasNext()) {
			String keyword = keywordSet.next();
			
			// Use this code if the you want filter some basic keywords like prepositions or words wich firts word isn't upper case
			/*if(Character.isUpperCase(keyword.charAt(0)) && keyword.length() >3 && !Character.isUpperCase(keyword.charAt(1))) {
				String keywordProccessed = keyword.replace(",", "");
				keywordProccessed = keywordProccessed.replace(":", "");
				keywordProccessed = keywordProccessed.replace(".", "");
				totalKeywordSet.add(keywordProccessed);
				System.out.println(keywordProccessed);
			}*/
			
			// Remove some characters. It can be improved by using a correct reg exp
			keyword = keyword.replaceAll("\"", "").replaceAll("\\?", "").replaceAll("\\)", "");
			if(keyword.contains("/")){
				for(String k : keyword.split("/")) {
					System.out.println(k);
					totalKeywordSet.add(k);
				}
			}else {
				System.out.println(keyword);
				totalKeywordSet.add(keyword);
			}
		}
		System.out.println(totalKeywordSet.size());
		
		//totalKeywordSet.add("Himno"); totalKeywordSet.add("Ayuda");totalKeywordSet.add("Sevilla"); 
		
		Calendar date = new GregorianCalendar();
		// reset hour, minutes, seconds and millis
		date.set(Calendar.HOUR_OF_DAY, 0);
		date.set(Calendar.MINUTE, 0);
		date.set(Calendar.SECOND, 0);
		date.set(Calendar.MILLISECOND, 0);
		
		Date today = date.getTime();
		//Long todayLong = today.getTime();
		
		date.add(Calendar.DAY_OF_MONTH, -6);
		Date previousDay = date.getTime();
		//Long lastWeekLong = lastWeek.getTime();
		
		// seven days iteration. It is useful in the context you don't launch this batch daily
		for (int i = 0; i <7 ; i++) {
			
			long initDay = previousDay.getTime();
			date.add(Calendar.DAY_OF_MONTH, 1);
			previousDay = date.getTime();
			long endDay = previousDay.getTime();
			
			// We search by date between today and tomorrow in mongoDB for each of the keywords
			Map<String,Integer> nTweets = new HashMap<String, Integer>();
			
			BasicDBObject andQuery = new BasicDBObject();;
			for(String keywordS : totalKeywordSet) {
				
				// Creation of the query: number of tweets of this day that contains the keyword
				andQuery = new BasicDBObject();
				List<BasicDBObject> andQuerySegments = new ArrayList<>();
				andQuerySegments.add(new BasicDBObject("creationDateLong", new BasicDBObject("$gte", initDay)));
				andQuerySegments.add(new BasicDBObject("creationDateLong", new BasicDBObject("$lt", endDay)));
				andQuerySegments.add(new BasicDBObject("text", new BasicDBObject("$regex", keywordS).append("$options", "i")));
				andQuery.put("$and", andQuerySegments);
				
				List<org.bson.Document> grantDocumentsForKeyWord = 
						(List<org.bson.Document>) tweetsCollection.find(andQuery).into(new ArrayList<org.bson.Document>());
				
				// We store the amount of tweets associated with the keyword
				nTweets.put(keywordS, grantDocumentsForKeyWord.size());
				if(nTweets.get(keywordS) > 0)
					System.out.println("For the kyword: " + keywordS + " we have " + nTweets.get(keywordS) + " ocurrences");
			}
			System.out.println(andQuery);
			
			String dateStr = statisticsFormat.format(initDay);
			
			// Now we introduce the keyword statistics in the DB
			
			System.out.println("Date: " + dateStr);
			
			for(String keywordM : nTweets.keySet()) {
				Integer nTweet = nTweets.get(keywordM);
				
				BasicDBObject query = new BasicDBObject();
				List<BasicDBObject> andQueryParams = new ArrayList<>();
				andQueryParams.add(new BasicDBObject("keyword", keywordM));
				andQueryParams.add(new BasicDBObject("date", dateStr));
				query.put("$and", andQueryParams);
				
				BasicDBObject update = new BasicDBObject();
				
				update.put("$set", new BasicDBObject("dateLong", initDay));
				update.put("$inc", new BasicDBObject("tweets", nTweet));
				
				
				FindOneAndUpdateOptions optionsU = new FindOneAndUpdateOptions().upsert(true);
				//Check if keyword-date combination exist in the database and update it. If not, insert new
				grantStatisticsCollection.findOneAndUpdate(query, update, optionsU);
				
				System.out.println("Keyword: " + keywordM + ", tweets: " + nTweet);
			}
			
		}
		//Date dateS = format.parse("Tue Dec 12 16:08:28 +0000 2017");
		
		//System.out.println("La fecha tiene codigo: " + dateS.getTime());	
	}
	
	
}
