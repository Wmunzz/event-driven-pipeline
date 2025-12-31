import json
import boto3

s3 = boto3.client("s3")

def lambda_handler(event, context):
    bucket = event["Records"][0]["s3"]["bucket"]["name"]
    key = event["Records"][0]["s3"]["object"]["key"]

    response = s3.get_object(Bucket=bucket, Key=key)
    data = json.loads(response["Body"].read())

    processed = {
        "device_id": data["device_id"],
        "value": data["value"],
        "timestamp": data["timestamp"],
        "status": "processed"
    }

    new_key = key.replace("raw/", "processed/")

    s3.put_object(
        Bucket=bucket,
        Key=new_key,
        Body=json.dumps(processed),
        ContentType="application/json"
    )

    return {"statusCode": 200}
