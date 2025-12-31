import json
import boto3
from datetime import datetime

s3 = boto3.client("s3")
BUCKET = "iot-raw-data-munees-2025"

def lambda_handler(event, context):
    response = s3.list_objects_v2(Bucket=BUCKET, Prefix="processed/")
    values = []

    if "Contents" in response:
        for obj in response["Contents"]:
            if obj["Key"].endswith(".json"):
                file = s3.get_object(Bucket=BUCKET, Key=obj["Key"])
                data = json.loads(file["Body"].read())
                values.append(data["value"])

    if not values:
        return {"statusCode": 200}

    summary = {
        "date": datetime.utcnow().strftime("%Y-%m-%d"),
        "total_records": len(values),
        "average_value": sum(values) / len(values),
        "max_value": max(values),
        "min_value": min(values)
    }

    report_key = f"reports/daily-summary-{summary['date']}.json"

    s3.put_object(
        Bucket=BUCKET,
        Key=report_key,
        Body=json.dumps(summary, indent=2),
        ContentType="application/json"
    )

    return {"statusCode": 200}
