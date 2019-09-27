#!/usr/bin/env node

// @ts-ignore: Cannot find declaration file
require('source-map-support/register');
const cdk = require('@aws-cdk/core');
const { CloudtrailPartitionerStack } = require('../lib/cloudtrail_partitioner-stack');

const app = new cdk.App();
new CloudtrailPartitionerStack(app, 'CloudtrailPartitionerStack');
