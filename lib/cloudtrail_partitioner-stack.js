const cdk = require('@aws-cdk/core');
const lambda = require('@aws-cdk/aws-lambda');
const logs = require('@aws-cdk/aws-logs');
const events = require('@aws-cdk/aws-events');
const targets = require('@aws-cdk/aws-events-targets');
const iam = require('@aws-cdk/aws-iam');
const cloudwatch = require('@aws-cdk/aws-cloudwatch');
const cloudwatch_actions = require('@aws-cdk/aws-cloudwatch-actions');
const sns = require('@aws-cdk/aws-sns');
const sns_subscription = require('@aws-cdk/aws-sns-subscriptions');

// Import libraries to read a config file
const yaml = require('js-yaml');
const fs = require('fs');

class CloudtrailPartitionerStack extends cdk.Stack {
  /**
   *
   * @param {cdk.Construct} scope
   * @param {string} id
   * @param {cdk.StackProps=} props
   */
  constructor(scope, id, props) {
    super(scope, id, props);

    // Load config file
    var config = yaml.safeLoad(fs.readFileSync('./config/config.yaml', 'utf8'));

    if (config['s3_bucket_containing_logs'] == 'MYBUCKET') {
      console.log("You must configure the CDK app by editing ./config/config.yaml");
      process.exit(1);
    }

    // Create Lambda
    const partitioner = new lambda.Function(this, "partitioner", {
      runtime: lambda.Runtime.PYTHON_3_7,
      code: lambda.Code.asset("resources/partitioner"),
      handler: "main.handler",
      description: "Partitions the Athena table for CloudTrail",
      logRetention: logs.RetentionDays.TWO_WEEKS,
      timeout: cdk.Duration.seconds(900),
      memorySize: 128,
      environment: {
        "S3_BUCKET_CONTAINING_LOGS": config['s3_bucket_containing_logs'],
        "CLOUDTRAIL_PREFIX": config["cloudtrail_prefix"],
        "PARTITION_DAYS": "1", // This is run nightly so only need to run partition for one day
        "OUTPUT_S3_BUCKET": config["output_s3_bucket"],
        "DATABASE": config["database"],
        "TABLE_PREFIX": config["table_prefix"]
      }
    });

    if (config['output_s3_bucket'] == "default") {
      // This is only used for the IAM policy, we leave this as *-* because there is not
      // an easy way of figuring out the AWS account from within the CDK
      config['output_s3_bucket'] = "aws-athena-query-results-*-*"
    }

    // Create rule to trigger this be run every 24 hours
    new events.Rule(this, "scheduled_run", {
      ruleName: "athena_partitioner_for_cloudtrail",
      // Run at 10pm EST (midnight UTC) every night
      schedule: events.Schedule.expression("cron(0 0 * * ? *)"),
      description: "Starts the CloudMapper auditing task every night",
      targets: [new targets.LambdaFunction(partitioner)]
    });

    // Grant access to Athena, Glue, and identifying the regions
    partitioner.addToRolePolicy(new iam.PolicyStatement({
      resources: ['*'],
      actions: [
        "athena:StartQueryExecution",
        "athena:GetQueryExecution",
        "athena:GetQueryResults",
        "glue:BatchCreatePartition",
        "glue:BatchGetPartition",
        "glue:CreateTable",
        "glue:CreateDatabase",
        "glue:GetDatabase",
        "glue:GetTable",
        "glue:UpdateTable",
        "ec2:DescribeRegions"
      ]
    }));

    // Grant access to list the bucket containing the CloudTrail logs
    partitioner.addToRolePolicy(new iam.PolicyStatement({
      resources: ['arn:aws:s3:::'+config['s3_bucket_containing_logs']],
      actions: [
        's3:ListBucket',
        's3:GetBucketLocation',
      ]
    }));

    // Grant access to the Athena query results
    partitioner.addToRolePolicy(new iam.PolicyStatement({
      resources: [
        'arn:aws:s3:::'+config['output_s3_bucket'],
        'arn:aws:s3:::'+config['output_s3_bucket']+"/*"
      ],
      actions: [
        "s3:GetBucketLocation",
        "s3:GetObject",
        "s3:ListBucket",
        "s3:ListBucketMultipartUploads",
        "s3:ListMultipartUploadParts",
        "s3:AbortMultipartUpload",
        "s3:CreateBucket",
        "s3:PutObject"
      ]
    }));

    // Create alarm for any errors
    const error_alarm =  new cloudwatch.Alarm(this, "error_alarm", {
      metric: new cloudwatch.Metric({
        namespace: 'cloudtrail_partitioner',
        metricName: "errors",
        statistic: "Sum"
      }),
      threshold: 0,
      evaluationPeriods: 1,
      datapointsToAlarm: 1,
      treatMissingData: cloudwatch.TreatMissingData.NOT_BREACHING,
      alarmDescription: "Detect errors",
      alarmName: "cloudtrail_partitioner_errors"
    });

    // Create SNS for alarms to be sent to
    const sns_topic = new sns.Topic(this, 'cloudtrail_partitioner_alarm', {
      displayName: 'cloudtrail_partitioner_alarm'
    });

    // Connect the alarm to the SNS
    error_alarm.addAlarmAction(new cloudwatch_actions.SnsAction(sns_topic));

    // Create Lambda to forward alarms
    const alarm_forwarder = new lambda.Function(this, "alarm_forwarder", {
      runtime: lambda.Runtime.PYTHON_3_7,
      code: lambda.Code.asset("resources/alarm_forwarder"),
      handler: "main.handler",
      description: "Forwards alarms from the local SNS to another",
      logRetention: logs.RetentionDays.TWO_WEEKS,
      timeout: cdk.Duration.seconds(30),
      memorySize: 128,
      environment: {
        "ALARM_SNS": config['alarm_sns_arn']
      },
    });

    // Add priv to publish the events so the alarms can be forwarded
    alarm_forwarder.addToRolePolicy(new iam.PolicyStatement({
      resources: [config['alarm_sns_arn']],
      actions: ['sns:Publish']
    }));

    // Connect the SNS to the Lambda
    sns_topic.addSubscription(new sns_subscription.LambdaSubscription(alarm_forwarder));
  }
}

module.exports = { CloudtrailPartitionerStack }
