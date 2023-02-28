package edu.upf;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import edu.upf.model.ExtendedSimplifiedTweet;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.StringTokenizer;

public class BiGramsApp {
    
    public static void main(String[] args) {
        List<String> argsList = Arrays.asList(args);
        String language = argsList.get(0);
        String outputFile = argsList.get(1);
        String input = argsList.get(2);

        SparkConf conf = new SparkConf().setAppName("BiGram App");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);
        JavaRDD<String> sentences = sparkContext.textFile(input);

        JavaPairRDD<String, Integer> tweets = sentences
            .flatMap(s -> Arrays.asList(s.split("\n")).iterator())
            .filter(tweet -> myFunction(tweet) != null)
            .map(BiGramsApp::myFunction)
            .filter(tweet -> tweet.getLanguage().equals(language))
            .filter(tweet -> !tweet.isRetweeted())
            .map(ExtendedSimplifiedTweet::getText)
            .flatMap(s -> makeBiGram(s).iterator())
            .mapToPair(word -> new Tuple2<>(word, 1))
            .reduceByKey(Integer::sum)
            .mapToPair(Tuple2::swap)
            .sortByKey(false)
            .mapToPair(Tuple2::swap);

        tweets.saveAsTextFile(outputFile);

    }

    public static ExtendedSimplifiedTweet myFunction (String tweet){
        Optional<ExtendedSimplifiedTweet> otweet = ExtendedSimplifiedTweet.fromJson(tweet);
        return otweet.orElse(null);
    }

    public static String normalise(String word){
        return word.trim().toLowerCase();
    }

    private static ArrayList<String> makeBiGram(String tweet){
        ArrayList<String> bigrams = new ArrayList<String>();
        StringTokenizer tokenizer = new StringTokenizer(tweet);
        if(tokenizer.countTokens() > 1){
            String s1 = "";
            String s2 = "";
            String s3 = "";
            
            while (tokenizer.hasMoreTokens()){
                if(s1.isEmpty())
                    s1 = normalise(tokenizer.nextToken());
                s2 = normalise(tokenizer.nextToken());
                s3 = "< " + s1 + " , " + s2 + " >";
                bigrams.add(s3);
                s1 = s2;
            }
        }
        return bigrams;
    }

}
