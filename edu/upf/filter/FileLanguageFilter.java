package edu.upf.filter;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter; 
import java.util.Optional;

import edu.upf.parser.SimplifiedTweet;

public class FileLanguageFilter implements LanguageFilter{
    final String inputFile;
    final String outputFile;

    public FileLanguageFilter (String inputFile, String outputFile){
        this.inputFile = inputFile;
        this.outputFile = outputFile;
    }
    @Override
    public void filterLanguage(String language) throws Exception {
        try(FileReader fr = new FileReader(inputFile);
            BufferedReader br = new BufferedReader(fr);
            FileWriter fw = new FileWriter(outputFile, true);
            BufferedWriter bw = new BufferedWriter(fw);) {
                
                String jsonLine = "1";
                int count = 0;
                while ((jsonLine = br.readLine()) != null){
                
                Optional<SimplifiedTweet> optionalTweet =  SimplifiedTweet.fromJson(jsonLine);
                if (optionalTweet.isPresent()){
                SimplifiedTweet tweet = optionalTweet.get();
                if (tweet.getLanguage().equals(language)){
                    bw.write(tweet.toString());
                    //change ro RDD
                    bw.newLine();
                    count++;

                }
            }
        }
        System.out.println("number of tweets: " + count);
        br.close();
        bw.close();

    }  catch(Exception e){
        e.printStackTrace();
        throw e; 
    }
        
    }

    
}
