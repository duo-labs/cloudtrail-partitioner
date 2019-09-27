This project sets up partitioned Athena tables for your CloudTrail logs and updates the partitions nightly.  As new AWS accounts begin sending you logs or new AWS regions come online, your paritions will always be up-to-date.   It is based on work by Alex Smolen in his post [Partitioning CloudTrail Logs in Athena](https://medium.com/@alsmola/partitioning-cloudtrail-logs-in-athena-29add93ee070).

You can immediately deploy the CDK app, but I recommend first running this manaully to ensure everything is configured, and also because running it manually will (by default) create 90 days of partitions, whereas the nightly CDK will not run until 0600 UTC, and will only create partitions for the current day and tomorrow.

Tables are created for each account as `cloudtrail_000000000000` and also a view is created that unions all these tables.

# Setup
Edit `config/config.yaml` to specify the S3 bucket containing your CloudTrail logs, the SNS to send alarms to (you must create one if you don't already have one) and any other configuraiton info.

Set up the initial tables and partitions for the past 90 days (it is ok if you don't have that many logs), by running:
```
cd resources/partitioner
pip3 install pyyaml boto3 -t .
python3 main.py
```

Then deploy the nightly Lambda from the root directory:
```
npm i
cdk deploy
```

If you haven't used the cdk before, you may need to run `cdk bootstrap aws://000000000000/us-east-1` (replacing your account ID and region) before running `cdk deploy`.

# Using Athena
To query your tables, use the AWS Console to get to the Athena service in the region where this was deployed.  From there, you can run a query such as:

```
SELECT useridentity.arn, errorcode, count(*) AS count
FROM cloudtrail
WHERE region = 'us-east-1' AND year = '2019' AND month = '09' AND day = '19' 
  AND errorcode != '' 
GROUP BY errorcode, useridentity.arn 
ORDER BY count DESC
LIMIT 50;
```

That query will show you the most common errors by user (technically by ARN for the session).

