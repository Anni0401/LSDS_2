package edu.upf;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import edu.upf.model.ExtendedSimplifiedTweet;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

public class MostRetweetedApp {
    public static void main(String[] args){
        String outputDir = args[0];
        String input = args[1];

        //Create a SparkContext to initialize
        SparkConf conf = new SparkConf().setAppName("Most Retweeted App");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);
        // Load input
        JavaRDD<String> sentences = sparkContext.textFile(input);

        List<Long> mostRetweetedUsers = sentences
                .flatMap(s -> Arrays.asList(s.split("\n")).iterator())
                .filter(tweet -> myFunction(tweet) != null)
                .map(MostRetweetedApp::myFunction)
                .filter(ExtendedSimplifiedTweet::isRetweeted)
                .map(ExtendedSimplifiedTweet::getRetweetedUserId)
                .mapToPair(userId -> new Tuple2<>(userId, 1))
                .reduceByKey(Integer::sum)
                .mapToPair(Tuple2::swap)
                .sortByKey(false)
                .mapToPair(Tuple2::swap)
                .keys()
                .take(10);

        JavaRDD<ExtendedSimplifiedTweet> mostRetweetedTweets = sentences
                .flatMap(s -> Arrays.asList(s.split("\n")).iterator())
                .filter(tweet -> myFunction(tweet) != null)
                .map(MostRetweetedApp::myFunction)
                .filter(t -> mostRetweetedUser(mostRetweetedUsers, t))
                .mapToPair(tweet -> new Tuple2<>(tweet, 1))
                .reduceByKey(Integer::sum)
                .mapToPair(Tuple2::swap)
                .sortByKey(false)
                .mapToPair(Tuple2::swap)
                .keys();

        JavaRDD<String> bestTweets = mostRetweetedTweets
                .groupBy(ExtendedSimplifiedTweet::getUserId)
                .map(t -> t._2.iterator().next())
                .map(ExtendedSimplifiedTweet::toString);

        bestTweets.saveAsTextFile(outputDir);
    }
    public static ExtendedSimplifiedTweet myFunction (String tweet){
        Optional<ExtendedSimplifiedTweet> otweet = ExtendedSimplifiedTweet.fromJson(tweet);
        return otweet.orElse(null);
    }

    public static boolean mostRetweetedUser(List<Long> mostUser, ExtendedSimplifiedTweet tweet){

        if(!mostUser.isEmpty()){
            for (Long uid : mostUser) {
                if (uid.equals(tweet.getUserId())) {
                    return true;
                }
            }
        }
        return false;
    }

}