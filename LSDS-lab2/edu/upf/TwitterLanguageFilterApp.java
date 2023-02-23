package spark;
package upf.edu;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;
import scala.Tuple2;
import upf.edu.model.SimplifiedTweet;

import java.util.Arrays;
import java.util.Optional;

public class FilterLanguage {
    
    public static void main(Strings[] args){

        List<String> argsList = Arrays.asList(args);
        String language = argsList.get(0);
        String outputFile = argsList.get(1);
        String bucket = argsList.get(2);


        SparkConf conf = new SparkConf().setAppName("Twitter Filter");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);

        JavaRDD<String> sentences = sparkContext.textFile(input);

        JavaPairRDD<String, Integer> tweets = sentences
            .flatMap(s -> Arrays.asList(s.split("[ ]")).iterator())
            .filter(tweet -> myFunction(tweet.getLanguage(tweet).equals(language)))
            .map(SimplifiedTweet::ToString);

        tweets.saveAsTextFile(outputFile);
    }
}
