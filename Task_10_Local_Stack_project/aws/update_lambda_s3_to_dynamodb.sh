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
aws --endpoint-url=http://localhost:4566 lambda update-function-code \
    --function-name s3-to-dynamodb \
    --zip-file fileb:///tmp/test_project/s3_to_dynamodb.zip \
    --timeout 900 \
    --memory-size 3072 \
rm -r /tmp/test_project
#rm /tmp/test_project/s3_to_dynamodb.zip
#rm /tmp/test_project/s3_to_dynamodb.py

aws --endpoint-url http://localhost:4566 s3api put-bucket-notification-configuration \
    --bucket helsinki-city-bikes \
    --notification-configuration file://bucket_config.json
