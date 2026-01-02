import json
import os
import boto3
from datetime import datetime
from decimal import Decimal

# ---------- CONFIGURACIÓN ----------
TABLE_NAME = "InventoryPGL"
SNS_TOPIC_ARN = "arn:aws:sns:us-east-1:XXXXXXXXXXXX:InventoryAlertsPGL" # SUSTITUIR XXXXXX por mi accountID
LOW_STOCK_THRESHOLD = 5

# ---------- CLIENTES AWS ----------
dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(TABLE_NAME)
sns = boto3.client("sns")

# ---------- UTILIDADES ----------
def decimal_to_float(obj):
    """
    Convierte Decimal a float para poder serializar a JSON
    """
    if isinstance(obj, list):
        return [decimal_to_float(i) for i in obj]
    if isinstance(obj, dict):
        return {k: decimal_to_float(v) for k, v in obj.items()}
    if isinstance(obj, Decimal):
        return float(obj)
    return obj

def response(status_code, body):
    return {
        "statusCode": status_code,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(body)
    }

def publish_sns(message, subject="Inventory Notification"):
    sns.publish(
        TopicArn=SNS_TOPIC_ARN,
        Message=message,
        Subject=subject
    )

# ---------- HANDLERS ----------
def get_products():
    data = table.scan()
    items = decimal_to_float(data.get("Items", []))
    return response(200, items)


def create_product(body):
    required_fields = {"productId", "name", "stock", "price"}
    if not required_fields.issubset(body):
        return response(400, {"error": "Missing required fields"})

    item = {
        "productId": body["productId"],
        "name": body["name"],
        "stock": int(body["stock"]),
        "price": Decimal(str(body["price"])),
        "lastUpdate": datetime.utcnow().isoformat()
    }

    table.put_item(Item=item)

    publish_sns(
        f"Product created: {item['productId']} - {item['name']}",
        "New product added"
    )

    return response(201, {"message": "Product created"})

def update_stock(product_id, body):
    if "stock" not in body:
        return response(400, {"error": "Missing stock value"})

    new_stock = int(body["stock"])

    result = table.update_item(
        Key={"productId": product_id},
        UpdateExpression="SET stock = :s, lastUpdate = :u",
        ExpressionAttributeValues={
            ":s": new_stock,
            ":u": datetime.utcnow().isoformat()
        },
        ReturnValues="ALL_NEW"
    )

    if new_stock < LOW_STOCK_THRESHOLD:
        publish_sns(
            f"Low stock alert for product {product_id}. Current stock: {new_stock}",
            "Low stock alert"
        )

    return response(200, {
        "message": "Stock updated",
        "product": decimal_to_float(result["Attributes"])
    })

# ---------- ENTRY POINT ----------
def lambda_handler(event, context):
    try:
        method = event.get("httpMethod")
        path = event.get("path")
        body = json.loads(event["body"]) if event.get("body") else {}

        if method == "GET" and path == "/products":
            return get_products()

        if method == "POST" and path == "/products":
            return create_product(body)

        if method == "PUT" and path.startswith("/products/"):
            product_id = path.split("/")[-1]
            return update_stock(product_id, body)

        return response(404, {"error": "Endpoint not found"})

    except Exception as e:
        return response(500, {"error": str(e)})