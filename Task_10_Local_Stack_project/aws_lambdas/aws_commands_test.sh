# run function
aws --endpoint-url=http://localhost:4566 lambda invoke --function-name on-upload-metrics outputfile.txt

# upload a file
aws --endpoint-url http://localhost:4566 s3api put-object --bucket helsinki-city-bikes --key output  --body data/outputfile.txt

# logs
aws --endpoint-url http://localhost:4566 logs describe-log-groups --query logGroups[*].logGroupName
aws --endpoint-url http://localhost:4566 logs describe-log-streams --log-group-name '/aws/lambda/on-upload-metrics' --query logStreams[*].logStreamName
aws --endpoint-url http://localhost:4566 logs get-log-events --log-group-name '/aws/lambda/on-upload-metrics' --log-stream-name '2023/05/02/[$LATEST]9125496fda12b7d3bae2f1bd68f7bb01'


aws --endpoint-url http://localhost:4566 sqs list-queues
aws --endpoint-url http://localhost:4566 sqs get-queue-attributes --queue-url http://localhost:4566/000000000000/s3-events --attribute-names All

# get user id
aws --endpoint-url http://localhost:4566 sts get-caller-identity

# message from sqs
aws --endpoint-url http://localhost:4566 sqs receive-message --queue-url http://localhost:4566/000000000000/s3-events

aws --endpoint-url http://localhost:4566 dynamodb put-item --table-name HelsinkiCityBikesMonthMetrics --item '{"Month": {"S": "No One You Know"}, "Metric": {"S": "Call Me Today"}}'


aws lambda list-event-source-mappings --function-name s3-to-dynamodb --event-source-arn arn:aws:sqs:us-east-1:000000000000:s3-events
