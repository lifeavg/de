# create bucket
aws --endpoint-url http://localhost:4566 iam create-role --role-name lambda-role --assume-role-policy-document file:///home/lifeavg/Documents/projects/de/Task_10_Local_Stack_project/role.json

# run function
aws --endpoint-url=http://localhost:4566 lambda invoke --function-name on-upload-metrics outputfile.txt

# upload a file
aws --endpoint-url http://localhost:4566 s3api put-object --bucket helsinki-city-bikes --key output  --body data/outputfile.txt

# logs
aws --endpoint-url http://localhost:4566 logs describe-log-groups --query logGroups[*].logGroupName
aws --endpoint-url http://localhost:4566 logs describe-log-streams --log-group-name '/aws/lambda/on-upload-metrics' --query logStreams[*].logStreamName
aws --endpoint-url http://localhost:4566 logs get-log-events --log-group-name '/aws/lambda/on-upload-metrics' --log-stream-name '2023/05/02/[$LATEST]9125496fda12b7d3bae2f1bd68f7bb01'
