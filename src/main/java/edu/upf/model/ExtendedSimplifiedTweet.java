package edu.upf.model;

import java.io.Serializable;
import java.util.Optional;
import com.google.gson.Gson;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;


public class ExtendedSimplifiedTweet implements Serializable{

  // All classes use the same instance
  private static JsonParser parser;

  private final long tweetId;			  // the id of the tweet ('id')
  private final String text;  		      // the content of the tweet ('text')
  private final long userId;			  // the user id ('user->id')
  private final String userName;		  // the user name ('user'->'name')
  private final long followersCount;
  private final String language;          // the language of a tweet ('lang')
  private final boolean isRetweeted;
  private final long retweetedUserId;
  private final long retweetedTweetId;
  private final long timestampMs;		  // seconduserIds from epoch ('timestamp_ms')

  public ExtendedSimplifiedTweet(long tweetId, String text, long userId, String userName, long followersCount, String language, boolean isRetweeted, long retweetedUserId, long retweetedTweetId, long timestampMs) {

    this.tweetId = tweetId;
    this.text = text;
    this.userId = userId;
    this.userName = userName;
    this.followersCount = followersCount;
    this.language = language;
    this.isRetweeted = isRetweeted;
    this.retweetedUserId = retweetedUserId;
    this.retweetedTweetId = retweetedTweetId;
    this.timestampMs = timestampMs;

  } 

/**
* Returns a {@link ExtendedSimplifiedTweet} from a JSON String.
* If parsing fails, for any reason, return an {@link Optional#empty()}
*
* @param jsonStr
* @return an {@link Optional} of a {@link ExtendedSimplifiedTweet}
*/

  public static Optional<ExtendedSimplifiedTweet> fromJson(String jsonStr) {
    try{
      
    JsonObject jo = new JsonParser().parse(jsonStr).getAsJsonObject();
    
    JsonElement tweetIdEle = jo.get("id");
    long tweetId = (long) tweetIdEle.getAsLong();

    JsonElement textEle = jo.get("text");
    String text = (String) textEle.getAsString();
    
    JsonElement languageEle = jo.get("lang");
    String language = (String) languageEle.getAsString();

    JsonElement timestampEle = jo.get("timestamp_ms");
    long timestampMs = (long) timestampEle.getAsLong();

   

    JsonElement userIdEle = jo.getAsJsonObject("user").get("id");
    long userId = (long) userIdEle.getAsLong();

    JsonElement userNameEle = jo.getAsJsonObject("user").get("name");
    String userName = (String) userNameEle.getAsString();

    JsonElement userFollowersEle = jo.getAsJsonObject("user").get("followers_count");
    long followersCount = (long) userFollowersEle.getAsLong();

    JsonElement retweetStatusEle = jo.get("retweeted_status");
    String retweet_status = (String) retweetStatusEle.getAsString();
    boolean isRetweeted = retweet_status != null;

    //if (isRetweeted){
      JsonElement userIdREle = jo.getAsJsonObject("retweeted_status").getAsJsonObject("user").get("id");
      long retweetedUserId = (long) userIdREle.getAsLong();

      JsonElement IdREle = jo.getAsJsonObject("retweeted_status").get("id");
      long retweetedTweetId = (long) IdREle.getAsLong();
    //}else{
      //long retweetedUserId = 0;
      //long retweetedTweetId = 0;

    //}


    if (text != null && tweetId != 0 && language != null && timestampMs != 0 && userId != 0 && userName != null){
      ExtendedSimplifiedTweet tweet = new ExtendedSimplifiedTweet(tweetId, text, userId, userName, followersCount, language, isRetweeted, retweetedUserId, retweetedTweetId, timestampMs);
      Optional<ExtendedSimplifiedTweet> tweetOutput = Optional.of(tweet);
    
      return tweetOutput;
      

    }else {
      return Optional.empty();
    }
  } catch (Exception e) {
    return Optional.empty();
  }

  }
  public String getLanguage(){
    return this.language;
  }

  @Override
  public String toString() {
    
    return new Gson().toJson(this);
  }
  public boolean isRetweeted(){
    return this.isRetweeted;
  }
  public String getText(){
    return this.text;
  }

  public long getRetweetedUserId(){
    return this.retweetedUserId;
  }

  public long getUserId(){
    return this.userId;
  }
  
}
