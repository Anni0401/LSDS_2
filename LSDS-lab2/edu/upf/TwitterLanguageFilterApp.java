package upf.edu;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;
import scala.Tuple2;
import upf.edu.model.SimplifiedTweet;

import java.util.Arrays;
import java.util.Optional;
import java.util.List; 

public class TwitterLanguageFilterApp {

    public static void main(String[] args) {
        List<String> argsList = Arrays.asList(args);
        String language = argsList.get(0);
        String outputFile = argsList.get(1);
        String input = argsList.get(2);


        SparkConf conf = new SparkConf().setAppName("Twitter Language Filter App");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);

        JavaRDD<String> sentences = sparkContext.textFile(input);
        JavaRDD<String> tweets = sentences
                .flatMap(s -> Arrays.asList(s.split("\n")).iterator())
                .filter(tweet -> myFunction(tweet) != null)
                .map(TwitterLanguageFilterApp::myFunction)
                .filter(tweet -> tweet.getLanguage().equals(language))
                .map(SimplifiedTweet::toString);

        tweets.saveAsTextFile(outputFile);

    }
    public static SimplifiedTweet myFunction (String tweet){
        Optional<SimplifiedTweet> otweet = SimplifiedTweet.fromJson(tweet);
        return otweet.orElse(null);
    }
}
