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

public class KeywordRatingGeneration {
	/**
	 * 
	 * This process generates keyword ratings between grants
	 */
	public static void main(String... args) throws Exception {

		// DB access used in the microservice secure section
		MongoClientURI uri = new MongoClientURI("mongodb://admin:passwordCurro@ds129386.mlab.com:29386/si1718-flp-grants-secure");

		// DB access used in the microservice basic section
		// MongoClientURI uri = new MongoClientURI("mongodb://curro:curro@ds149855.mlab.com:49855/si1718-flp-grants");

		MongoClient client = new MongoClient(uri);
		MongoDatabase db = client.getDatabase(uri.getDatabase());

		// Retrieving mongoDB collections

		MongoCollection<org.bson.Document> grantsCollection = db.getCollection("grants");
		MongoCollection<org.bson.Document> grantRatingsCollection = db.getCollection("grantRatings");

		List<org.bson.Document> grantDocuments = (List<org.bson.Document>) grantsCollection.find()
				.into(new ArrayList<org.bson.Document>());

		Map<List<String>, Double> ratings = new HashMap<List<String>, Double>();

		/*
		 * while(grantDocuments.hasNext()) { String keyword = keywordSet.next();
		 * if(Character.isUpperCase(keyword.charAt(0)) && keyword.length() >3 &&
		 * !Character.isUpperCase(keyword.charAt(1))) { String keywordProccessed =
		 * keyword.replace(",", ""); keywordProccessed = keywordProccessed.replace(":",
		 * ""); keywordProccessed = keywordProccessed.replace(".", "");
		 * totalKeywordSet.add(keywordProccessed);
		 * System.out.println(keywordProccessed); } }
		 */

		
		int count = 0;
		for (int i = 0; i < grantDocuments.size(); i++) {
			Document grant1 = grantDocuments.get(i);
			if (grant1.get("idGrant") == null || grant1.get("idGrant") == "null")
				continue;
			List<org.bson.Document> grantSet = new ArrayList<>();
			System.out.println("Ratings quantity introduced until now: " + count);
			System.out.println("Next grant: number " + i + ", with id: " + grant1.get("idGrant"));
			System.out.println("-----------------------------------------------------");
			for (int j = 1 + i; j < grantDocuments.size(); j++) { // Only half matrix is generated because rating generated is symmetrical

				Document grant2 = grantDocuments.get(j);
				if (grant2.get("idGrant") == null || grant2.get("idGrant") == "null")
					continue;

				List<String> keywords1 = clearKeywordsWithMassiveResults((List<String>) grant1.get("keywords"));

				List<String> keywords2 = clearKeywordsWithMassiveResults((List<String>) grant2.get("keywords"));
				
				/* Rating calculus. A coincidence in keywords between two grants adds two points. 
				 * The sum is divided by the sum of both keywords list size. In addition, this value
				 * is converted in a 1-5 value 
				 */
				Double rating = 0.0;
				if (keywords1.size() > 0 && keywords2.size() > 0) {
					for (String k : keywords1) {
						if (keywords2.contains(k))
							rating += 2.0;
					}
					rating = rating / ((keywords1.size() + keywords2.size()) * 1.0);

					if (rating != 0) {
						Document ratingDocument = new Document();
						ratingDocument.put("idGrantA", grant1.get("idGrant"));
						ratingDocument.put("idGrantB", grant2.get("idGrant"));
						ratingDocument.put("rating", rating * 4 + 1);

						grantSet.add(ratingDocument);
						
						//This query prevent insert duplicate elements with upsert. However, it spent an excessive time.
						/*
						 * // Goal: query sentence: (idGrantA = idGrant1 && idGrantB = idGrant2) ||
						 * (idGrantA = idGrant2 && idGrantB = idGrant1) BasicDBObject orQuery = new
						 * BasicDBObject(); List<BasicDBObject> orQuerySegments = new ArrayList<>();
						 * 
						 * // First "and" (idGrantA = idGrant1 && idGrantB = idGrant2) BasicDBObject
						 * andQuery1 = new BasicDBObject(); List<BasicDBObject> andQuerySegments1 = new
						 * ArrayList<>(); andQuerySegments1.add(new BasicDBObject("idGrantA",
						 * grant1.get("idGrant"))); andQuerySegments1.add(new BasicDBObject("idGrantB",
						 * grant2.get("idGrant"))); andQuery1.put("$and", andQuerySegments1);
						 * 
						 * orQuerySegments.add(andQuery1);
						 * 
						 * // Second "and" (idGrantA = idGrant2 && idGrantB = idGrant1) BasicDBObject
						 * andQuery2 = new BasicDBObject(); List<BasicDBObject> andQuerySegments2 = new
						 * ArrayList<>(); andQuerySegments2.add(new BasicDBObject("idGrantA",
						 * grant2.get("idGrant"))); andQuerySegments2.add(new BasicDBObject("idGrantB",
						 * grant1.get("idGrant"))); andQuery2.put("$and", andQuerySegments2);
						 * 
						 * orQuerySegments.add(andQuery2);
						 * 
						 * orQuery.put("$or", orQuerySegments);
						 * 
						 * // UPSERT FindOneAndReplaceOptions options = new
						 * FindOneAndReplaceOptions().upsert(true);
						 * grantRatingsCollection.findOneAndReplace(orQuery, ratingDocument, options);
						 */

						// System.out.println("(" + i + "," + j + ")");
						// System.out.println("Va por el rating (" + grant1.get("idGrant") + "," +
						// grant2.get("idGrant") + ")");

						count++;
					}
				}
			}
			if (grantSet.size() != 0)
				grantRatingsCollection.insertMany(grantSet);
		}
		System.out.println("Finished. Total: " + count);

	}
	
	// Used to eliminate very common keywords because mLab limit the space in a free account
	private static List<String> clearKeywordsWithMassiveResults(List<String> keywords) {
		List<String> clearKeywords = new ArrayList<>();
		for (String keyword : keywords) {
			if (!keyword.contains(" ") && !keyword.equals("Incentivo")) {

				clearKeywords.add(keyword);
			}
		}
		return clearKeywords;
	}
	
	// Use this method if keywords need to be filtered or processed
	private static List<String> clearKeywords(List<String> keywords) {
		List<String> clearKeywords = new ArrayList<>();
		for (String keyword : keywords) {
			if (Character.isUpperCase(keyword.charAt(0)) && keyword.length() > 3
					&& !Character.isUpperCase(keyword.charAt(1))) {
				String keywordProccessed = keyword.replace(",", "");
				keywordProccessed = keywordProccessed.replace(":", "");
				keywordProccessed = keywordProccessed.replace(".", "");
				clearKeywords.add(keywordProccessed);
			}
		}
		return clearKeywords;
	}
}
