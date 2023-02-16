package edu.upf.uploader;

import java.util.List;
import java.io.File;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.PutObjectRequest;


public class S3Uploader implements Uploader {

    final String bucketName;
    final String prefix;
    private AmazonS3 s3Client;

    public S3Uploader(String bucketName, String prefix, AmazonS3 s3client){
        this.bucketName = bucketName;
        this.prefix = prefix;
        AmazonS3 credentials = AmazonS3ClientBuilder.standard().withRegion(Regions.US_EAST_1).build();
        this.s3Client = credentials;
    }

    public boolean ExistingBucket(){

        if (s3Client.doesBucketExistV2(bucketName)){
            return true;
        } else {
            s3Client.createBucket(bucketName);
            return true;
        }
    }

    @Override
    public void upload(List<String> files) {
        if (ExistingBucket() == false){
            System.out.println("The bucket does not exist");
        } else {
            for (String file:files){
                File uploadingfile = new File(file);
                PutObjectRequest request = new PutObjectRequest(bucketName, prefix + uploadingfile.getName(), new File(file));
                s3Client.putObject(request);
            }
        }
    }
}
