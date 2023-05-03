zip on_upload_metrics.zip on_upload_metrics.py
aws --endpoint-url=http://localhost:4566 lambda update-function-code --function-name on-upload-metrics --zip-file fileb:///home/lifeavg/Documents/projects/de/Task_10_Local_Stack_project/on_upload_metrics.zip
rm on_upload_metrics.zip
aws --endpoint-url http://localhost:4566 s3api put-bucket-notification-configuration --bucket helsinki-city-bikes --notification-configuration '{"LambdaFunctionConfigurations":[{"Id":"on-upload-metrics","LambdaFunctionArn":"arn:aws:lambda:us-east-1:000000000000:function:on-upload-metrics","Events":["s3:ObjectCreated:*"]}]}'
