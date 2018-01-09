package data.streaming.dto;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Locale;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 *
 * POJO to represent a tweet which is a Tuple4<Name, Text, Date,Language>
 *
 */
@JsonIgnoreProperties(ignoreUnknown=true)
public class TweetDTO{
	@JsonProperty("created_at")
	private String createdAt;
	private TweetUserDTO user;
	private String text;
	@JsonProperty("lang")
	private String language;

	public TweetDTO(String createdAt, TweetUserDTO user, String text, String language) throws ParseException {
		super();
		this.createdAt = createdAt;
		this.user = user;
		this.text = text;
		this.language = language;
	}
	
	

	public TweetDTO() {
		super();
	}


	public TweetUserDTO getUser() {
		return user;
	}

	public String getText() {
		return text;
	}

	public String getCreatedAt() {
		return createdAt;
	}

	public String getLanguage() {
		return language;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((createdAt == null) ? 0 : createdAt.hashCode());
		result = prime * result + ((user == null) ? 0 : user.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		TweetDTO other = (TweetDTO) obj;
		if (createdAt == null) {
			if (other.createdAt != null)
				return false;
		} else if (!createdAt.equals(other.createdAt))
			return false;
		if (user == null) {
			if (other.user != null)
				return false;
		} else if (!user.equals(other.user))
			return false;
		return true;
	}

	public String toString() {
		return "TweetDTO [user=" + user + ", text=" + text + ", createdAt=" + createdAt + ", language=" + language
				+ "]";
	}

	
	

}