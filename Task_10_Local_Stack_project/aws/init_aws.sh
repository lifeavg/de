echo ---------------- create-bucket
aws --endpoint-url http://localhost:4566 s3api create-bucket \
    --bucket helsinki-city-bikes \
    --region us-east-1

echo ---------------- create-role lambda-s3-role
aws --endpoint-url http://localhost:4566 iam create-role \
    --role-name lambda-s3-role \
    --assume-role-policy-document file://role.json

echo ---------------- create-function on-upload-metrics
zip -q /tmp/on_upload_metrics.zip on_upload_metrics.py
aws --endpoint-url=http://localhost:4566 lambda create-function \
    --function-name on-upload-metrics \
    --handler on_upload_metrics.on_upload_metrics \
    --runtime python3.9 --role arn:aws:iam::000000000000:role/lambda-s3-role \
    --zip-file fileb:///tmp/on_upload_metrics.zip
rm /tmp/on_upload_metrics.zip

echo ---------------- create-queue s3-events
aws --endpoint-url http://localhost:4566 sqs create-queue \
    --queue-name s3-events \
    --attributes file://queue_config.json
aws --endpoint-url http://localhost:4566 sqs add-permission \
    --queue-url http://localhost:4566/000000000000/s3-events \
    --label SendMessagesFromS3Events \
    --aws-account-ids AKIAIOSFODNN7EXAMPLE \
    --actions SendMessage

echo ---------------- put-bucket-notification-configuration
aws --endpoint-url http://localhost:4566 s3api put-bucket-notification-configuration \
    --bucket helsinki-city-bikes \
    --notification-configuration file://bucket_config.json

echo ---------------- dynamodb create-table HelsinkiCityBikes
aws --endpoint-url http://localhost:4566 dynamodb create-table \
    --table-name HelsinkiCityBikes \
    --attribute-definitions \
        AttributeName=Departure,AttributeType=S \
        AttributeName=Index,AttributeType=S \
    --key-schema \
        AttributeName=Departure,KeyType=HASH \
        AttributeName=Index,KeyType=RANGE \
    --provisioned-throughput \
        ReadCapacityUnits=5,WriteCapacityUnits=40000

echo ---------------- dynamodb create-table HelsinkiCityBikesStationMonthMetrics
aws --endpoint-url http://localhost:4566 dynamodb create-table \
    --table-name HelsinkiCityBikesStationMonthMetrics \
    --attribute-definitions \
        AttributeName=Month,AttributeType=S \
        AttributeName=StationId,AttributeType=S \
    --key-schema \
        AttributeName=Month,KeyType=HASH \
        AttributeName=StationId,KeyType=RANGE \
    --provisioned-throughput \
        ReadCapacityUnits=5,WriteCapacityUnits=50

echo ---------------- dynamodb create-table HelsinkiCityBikesDayMetrics
aws --endpoint-url http://localhost:4566 dynamodb create-table \
    --table-name HelsinkiCityBikesDayMetrics \
    --attribute-definitions \
        AttributeName=Day,AttributeType=S \
    --key-schema \
        AttributeName=Day,KeyType=HASH \
    --provisioned-throughput \
        ReadCapacityUnits=5,WriteCapacityUnits=500

echo ---------------- create-role lambda-sqs-role
aws --endpoint-url http://localhost:4566 iam create-role \
    --role-name lambda-sqs-role \
    --assume-role-policy-document file://role.json

echo ---------------- create-function s3-to-dynamodb
mkdir /tmp/test_project
wget -q -P /tmp/test_project https://files.pythonhosted.org/packages/e9/d7/ee1b27176addc1236f4a59a9ca105bbdf60424a597ab9b4e13f09e0a816f/pandas-2.0.1-cp39-cp39-manylinux_2_17_x86_64.manylinux2014_x86_64.whl
wget -q -P /tmp/test_project https://files.pythonhosted.org/packages/83/be/de078ac5e4ff572b1bdac1808b77cea2013b2c6286282f89b1de3e951273/numpy-1.24.3-cp39-cp39-manylinux_2_17_x86_64.manylinux2014_x86_64.whl
wget -q -P /tmp/test_project https://files.pythonhosted.org/packages/7f/99/ad6bd37e748257dd70d6f85d916cafe79c0b0f5e2e95b11f7fbc82bf3110/pytz-2023.3-py2.py3-none-any.whl
wheel unpack -d /tmp/test_project /tmp/test_project/pandas-2.0.1-cp39-cp39-manylinux_2_17_x86_64.manylinux2014_x86_64.whl
wheel unpack -d /tmp/test_project /tmp/test_project/numpy-1.24.3-cp39-cp39-manylinux_2_17_x86_64.manylinux2014_x86_64.whl
wheel unpack -d /tmp/test_project /tmp/test_project/pytz-2023.3-py2.py3-none-any.whl
mv /tmp/test_project/numpy-1.24.3/* /tmp/test_project
mv /tmp/test_project/pandas-2.0.1/* /tmp/test_project
mv /tmp/test_project/pytz-2023.3/* /tmp/test_project
rmdir /tmp/test_project/numpy-1.24.3
rmdir /tmp/test_project/pandas-2.0.1
rmdir /tmp/test_project/pytz-2023.3
rm /tmp/test_project/pandas-2.0.1-cp39-cp39-manylinux_2_17_x86_64.manylinux2014_x86_64.whl
rm /tmp/test_project/numpy-1.24.3-cp39-cp39-manylinux_2_17_x86_64.manylinux2014_x86_64.whl
rm /tmp/test_project/pytz-2023.3-py2.py3-none-any.whl
cp s3_to_dynamodb.py /tmp/test_project/s3_to_dynamodb.py
cd /tmp/test_project
zip -q -r s3_to_dynamodb.zip *
cd $OLDPWD
aws --endpoint-url=http://localhost:4566 lambda create-function \
    --function-name s3-to-dynamodb \
    --handler s3_to_dynamodb.s3_to_dynamodb \
    --runtime python3.9 --role arn:aws:iam::000000000000:role/lambda-sqs-role \
    --timeout 900 \
    --memory-size 3072 \
    --zip-file fileb:///tmp/test_project/s3_to_dynamodb.zip
rm -r /tmp/test_project

echo ---------------- create-event-source-mapping s3-to-dynamodb
aws --endpoint-url=http://localhost:4566 lambda create-event-source-mapping \
    --function-name s3-to-dynamodb \
    --batch-size 10 \
    --event-source-arn arn:aws:sqs:us-east-1:000000000000:s3-events
