# assumerolespark-s3
assumerolespark-s3

Usecase:

   You can pass credential to assume role and read/write the file for specific bucket and use instance profile for other buckets. Example, if you want to read the logs from master account and write to test account then analysis it. 
   
 
 Steps: (tested on emr 5.x)
    1. Build the jar and upload to s3 
     gradle build 
     
    2. Configure EMR to add above jar in emrfs 
     
     https://aws.amazon.com/blogs/big-data/securely-analyze-data-from-another-aws-account-with-emrfs/
    
    3. Configure spark context with required valued 
    
spark.sparkContext.hadoopConfiguration.set("AWS_ACCESS_KEY","<access key>")
spark.sparkContext.hadoopConfiguration.set("AWS_SECRET_KEY_ID","<secret key>)
spark.sparkContext.hadoopConfiguration.set("AWS_SESSION_TOKEN","<session key>")
spark.sparkContext.hadoopConfiguration.set("amz-assume-role-arn","<role need to be assumed>")
spark.sparkContext.hadoopConfiguration.set("s3_bucket_uri","<bucketuri>")

  4. All set, you can run spark code which uses assume role for bucket uri and instance profile for others 
     
    
