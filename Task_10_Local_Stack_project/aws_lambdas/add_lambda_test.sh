aws --endpoint-url http://localhost:4566 s3api create-bucket --bucket helsinki-city-bikes --region us-east-1
aws --endpoint-url http://localhost:4566 iam create-role --role-name lambda-role --assume-role-policy-document file:///home/lifeavg/Documents/projects/de/Task_10_Local_Stack_project/role.json
zip on_upload_metrics.zip on_upload_metrics.py
aws --endpoint-url=http://localhost:4566 lambda create-function --function-name on-upload-metrics --handler on_upload_metrics.on_upload_metrics --runtime python3.9 --role arn:aws:iam::000000000000:role/lambda-role --zip-file fileb:///home/lifeavg/Documents/projects/de/Task_10_Local_Stack_project/on_upload_metrics.zip
rm on_upload_metrics.zip
aws --endpoint-url http://localhost:4566 s3api put-bucket-notification-configuration --bucket helsinki-city-bikes --notification-configuration '{"LambdaFunctionConfigurations":[{"Id":"on-upload-metrics","LambdaFunctionArn":"arn:aws:lambda:us-east-1:000000000000:function:on-upload-metrics","Events":["s3:ObjectCreated:*"]}]}'
