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

public class TestBatch {
	
	public static void main(String... args) throws Exception {
		MongoClientURI uri = new MongoClientURI("mongodb://admin:passwordCurro@ds129386.mlab.com:29386/si1718-flp-grants-secure");
		//MongoClientURI uri = new MongoClientURI("mongodb://curro:curro@ds149855.mlab.com:49855/si1718-flp-grants");
		MongoClient client = new MongoClient(uri);
		MongoDatabase db = client.getDatabase(uri.getDatabase());
		
		//For Twitter dates:
		SimpleDateFormat format = new SimpleDateFormat("EEE MMM dd HH:mm:ss ZZZZZ yyyy",Locale.ENGLISH);
		format.setLenient(true);
		
		// Date format for statistics
		SimpleDateFormat statisticsFormat = new SimpleDateFormat("yyyy-MM-dd");
		
		// Retrieving a collection
	
		MongoCollection<org.bson.Document> tweetsCollection = db.getCollection("tweets");
		MongoCollection<org.bson.Document> grantsCollection = db.getCollection("grants");
		MongoCollection<org.bson.Document> grantStatisticsCollection = db.getCollection("grantStatistics");
		
		//List<org.bson.Document> grantDocuments = (List<org.bson.Document>) grantsCollection.find().into(new ArrayList<org.bson.Document>());
		//List<org.bson.Document> tweetsDocuments = (List<org.bson.Document>) tweetsCollection.find().into(new ArrayList<org.bson.Document>());
		
		
		MongoCursor<String> keywordSet = grantsCollection.distinct("keywords", String.class).iterator();
		
		
		Set<String> totalKeywordSet = new HashSet<String>();
		
		while(keywordSet.hasNext()) {
			String keyword = keywordSet.next();
			/*if(Character.isUpperCase(keyword.charAt(0)) && keyword.length() >3 && !Character.isUpperCase(keyword.charAt(1))) {
				String keywordProccessed = keyword.replace(",", "");
				keywordProccessed = keywordProccessed.replace(":", "");
				keywordProccessed = keywordProccessed.replace(".", "");
				totalKeywordSet.add(keywordProccessed);
				System.out.println(keywordProccessed);
			}*/
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
		
		// Iteramos por siete d�as
		
		
		for (int i = 0; i <7 ; i++) {
			
			long initDay = previousDay.getTime();
			date.add(Calendar.DAY_OF_MONTH, 1);
			previousDay = date.getTime();
			long endDay = previousDay.getTime();
			
			// Buscamos por fecha entre hoy y ma�ana en mongoDB para cada una de las keywords
			Map<String,Integer> nTweets = new HashMap<String, Integer>();
			
			BasicDBObject andQuery = new BasicDBObject();;
			for(String keywordS : totalKeywordSet) {
				
				// Creacion de la query: numero de tweets de hoy y que tenga la keyword 
				andQuery = new BasicDBObject();
				List<BasicDBObject> andQuerySegments = new ArrayList<>();
				andQuerySegments.add(new BasicDBObject("creationDateLong", new BasicDBObject("$gte", initDay)));
				andQuerySegments.add(new BasicDBObject("creationDateLong", new BasicDBObject("$lt", endDay)));
				andQuerySegments.add(new BasicDBObject("text", new BasicDBObject("$regex", keywordS).append("$options", "i")));
				andQuery.put("$and", andQuerySegments);
				
				List<org.bson.Document> grantDocumentsForKeyWord = 
						(List<org.bson.Document>) tweetsCollection.find(andQuery).into(new ArrayList<org.bson.Document>());
				
				// almacenamos la cantidad de tweets asociado al keyword
				nTweets.put(keywordS, grantDocumentsForKeyWord.size());
				if(nTweets.get(keywordS) > 0)
					System.out.println("Para la key: " + keywordS + " tenemos " + nTweets.get(keywordS) + " ocurrencias");
			}
			System.out.println(andQuery);
			
			String dateStr = statisticsFormat.format(initDay);
			
			// Ahora introducimos las keywords en la db
			
			
			System.out.println("Para la fecha: " + dateStr);
			
			for(String keywordM : nTweets.keySet()) {
				Integer nTweet = nTweets.get(keywordM);
				
				/*
				Document documentDetail = new Document();
				documentDetail.put("idKeywordStatistic", keywordM + "-" + dateStr);
				documentDetail.put("keyword", keywordM);
				documentDetail.put("tweets", nTweet);
				documentDetail.put("date", dateStr);
				documentDetail.put("dateLong", initDay);
				
				FindOneAndReplaceOptions options = new FindOneAndReplaceOptions().upsert(true);
				grantStatisticsCollection.findOneAndReplace(new BasicDBObject("idKeywordStatistic", keywordM + "-" + dateStr), documentDetail, options);
				*/
				
				
				BasicDBObject query = new BasicDBObject();
				List<BasicDBObject> andQueryParams = new ArrayList<>();
				andQueryParams.add(new BasicDBObject("keyword", keywordM));
				andQueryParams.add(new BasicDBObject("date", dateStr));
				query.put("$and", andQueryParams);
				
				BasicDBObject update = new BasicDBObject();
				
				update.put("$set", new BasicDBObject("dateLong", initDay));
				update.put("$inc", new BasicDBObject("tweets", nTweet));
				
				
				FindOneAndUpdateOptions optionsU = new FindOneAndUpdateOptions().upsert(true);
				grantStatisticsCollection.findOneAndUpdate(query, update, optionsU);
				
				System.out.println("Keyword: " + keywordM + ", tweets: " + nTweet);
			}
			
		}
		//Date dateS = format.parse("Tue Dec 12 16:08:28 +0000 2017");
		
		//System.out.println("La fecha tiene codigo: " + dateS.getTime());	
	}
	
	
}
