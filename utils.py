import uuid
import boto3
import os
from typing import Optional
from datetime import datetime
import io


def generate_unique_image_id() -> str:
    """
    Generate a unique UUID for image uploads.

    Returns:
        str: A unique UUID string
    """
    return str(uuid.uuid4())


def generate_s3_image_path(image_id: str, file_extension: str = "png") -> str:
    """
    Generate an S3 path for image storage.

    Args:
        image_id (str): Unique identifier for the image
        folder (str): S3 folder/prefix
        file_extension (str): File extension (default: "png")

    Returns:
        str: S3 key path for the image
    """
    # Combine folder, date path, and filename
    filename = f"{image_id}.{file_extension}"
    s3_key = f"{os.getenv('S3_FOLDER_NAME')}/{filename}"

    return s3_key


def upload_image_to_s3(
    image_buffer: io.BytesIO,
    bucket_name: str,
    s3_key: str,
    content_type: str = "image/png",
) -> dict:
    """
    Upload an image buffer to S3.

    Args:
        image_buffer (io.BytesIO): Image data buffer
        bucket_name (str): S3 bucket name
        s3_key (str): S3 object key/path
        content_type (str): MIME type of the image (default: "image/png")

    Returns:
        dict: Upload result with bucket and key information
    """
    try:
        # Initialize S3 client
        # If running on EC2 with IAM roles, credentials are optional
        aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
        aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")

        if aws_access_key_id and aws_secret_access_key:
            # Use explicit credentials
            s3_client = boto3.client(
                "s3",
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
                region_name=os.getenv("AWS_REGION"),
            )
        else:
            # Use IAM role (for EC2 instances) or default credential chain
            s3_client = boto3.client(
                "s3",
            )

        # Reset buffer position to beginning
        image_buffer.seek(0)

        # Upload to S3 without ACL (bucket must be configured for public access if needed)
        s3_client.upload_fileobj(
            image_buffer,
            bucket_name,
            s3_key,
            ExtraArgs={
                "ContentType": content_type,
            },
        )

        # Generate public URL
        public_url = f"https://{os.getenv('AWS_CLOUDFRONT_DISTRIBUTION_URL')}/{s3_key}"

        return {
            "success": True,
            "bucket": bucket_name,
            "key": s3_key,
            "public_url": public_url,
            "message": "Image uploaded successfully to S3",
        }

    except Exception as e:
        return {
            "success": False,
            "error": f"Failed to upload image to S3: {str(e)}",
            "bucket": bucket_name,
            "key": s3_key,
        }
