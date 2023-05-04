zip /tmp/on_upload_metrics.zip on_upload_metrics.py
aws --endpoint-url=http://localhost:4566 lambda update-function-code \
    --function-name on-upload-metrics \
    --zip-file fileb:///tmp/on_upload_metrics.zip
rm /tmp/on_upload_metrics.zip

aws --endpoint-url http://localhost:4566 s3api put-bucket-notification-configuration \
    --bucket helsinki-city-bikes \
    --notification-configuration file://bucket_config.json
