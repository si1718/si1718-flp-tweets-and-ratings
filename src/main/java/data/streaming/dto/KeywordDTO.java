package data.streaming.dto;

public class KeywordDTO {

	String key;
	Double statistic;

	public KeywordDTO(String key, Double statistic) {
		super();
		this.key = key;
		this.statistic = statistic;
	}

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public Double getStatistic() {
		return statistic;
	}

	public void setStatistic(Double statistic) {
		this.statistic = statistic;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((key == null) ? 0 : key.hashCode());
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
		KeywordDTO other = (KeywordDTO) obj;
		if (key == null) {
			if (other.key != null)
				return false;
		} else if (!key.equals(other.key))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "KeywordDTO [key=" + key + ", statistic=" + statistic + "]";
	}

}
