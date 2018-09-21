# LiverampDataIngestion
Spark ETL Application which process the unzipped files from staging location to generate the System of Record for Incremental files


## Run
```
1. ssh to the EC2 instance
2. Make sure jar files and config places are placed in right location
3. Run below spark submit command

spark-submit --packages com.typesafe:config:1.3.1,org.postgresql:postgresql:9.4.1212 --class com.ignitionone.idrlr.ThirdPartyFileIngest /home/hadoop/Jarfiles/idr-liveramp-data-ingestion-1.0.0-SNAPSHOT.jar --deploy-mode cluster --master yarn
```

## Testing
1. ssh to the EC2 instance
2. Make sure jar files and config places are placed in right location
3. Run below spark submit command

## Cron Script in Prod
0 2 * * * spark-submit --jars /home/hadoop/dependencies/idr-utilities-auditstore.jar --packages com.typesafe:config:1.3.1,org.postgresql:postgresql:9.4.1212 --class com.ignitionone.idrlr.ThirdPartyFileIngest /home/hadoop/liveramp_data_ingestion/idr-liveramp-data-ingestion.jar --deploy-mode cluster --master yarn >> /var/log/idr-liveramp-ingestion.log 2>&1

```
1. Check the S3 Key space to validate if the files are generated in the right order
2. Check the record count for no. of Nodes
3. Validate the Audit Store table
4. Run Automation Scripts
```


## Octopus Variables
* application.conf
    - #{Pg.Host} = Audit Store Postgres Hostname
    - #{Pg.Database} = Audit Store Postgres Database Name
    - #{Pg.User} = Audit Store Postgres Username
    - #{Pg.Password} = Audit Store Postgres Password
    - #{Aws.S3.BucketName} = AWS S3 Source Staging bucket name
    - #{Aws.S3.Prefix} = AWS S3 Source Staging Location Prefix
    - #{Aws.S3.Destination} = AWS S3 Destination Location for LV SOR
    - #{Aws.S3.Source} = AWS S3 Source Location
    - #{Size.Of.File} = Desired Size in MB for each file


