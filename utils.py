import uuid
import boto3
import os
from typing import Optional
from datetime import datetime
import io
from logging_config import (
    main_logger, 
    perf_logger, 
    log_execution_time, 
    log_sync_operation,
    performance_metrics
)

@log_execution_time("generate_unique_image_id", main_logger)
def generate_unique_image_id() -> str:
    """
    Generate a unique UUID for image uploads.

    Returns:
        str: A unique UUID string
    """
    image_id = str(uuid.uuid4())
    main_logger.debug(f"Generated unique image ID: {image_id}")
    return image_id

@log_execution_time("generate_s3_image_path", main_logger)
def generate_s3_image_path(image_id: str, file_extension: str = "png") -> str:
    """
    Generate an S3 path for image storage.

    Args:
        image_id (str): Unique identifier for the image
        file_extension (str): File extension (default: "png")

    Returns:
        str: S3 key path for the image
    """
    main_logger.debug(f"Generating S3 path for image ID: {image_id}, extension: {file_extension}")
    
    # Combine folder, date path, and filename
    filename = f"{image_id}.{file_extension}"
    s3_key = f"{os.getenv('S3_FOLDER_NAME')}/{filename}"
    
    main_logger.debug(f"Generated S3 key: {s3_key}")
    return s3_key

@log_execution_time("upload_image_to_s3", perf_logger)
def upload_image_to_s3(
    image_buffer: io.BytesIO,
    content_type: str = "image/png",
) -> dict:
    """
    Upload an image buffer to S3.

    Args:
        image_buffer (io.BytesIO): Image data buffer
        content_type (str): MIME type of the image (default: "image/png")

    Returns:
        dict: Upload result with bucket and key information
    """
    bucket_name = os.getenv("S3_BUCKET_NAME")
    aws_region = os.getenv("AWS_REGION")
    cloudfront_url = os.getenv("AWS_CLOUDFRONT_DISTRIBUTION_URL")
    
    main_logger.info(f"Starting S3 image upload to bucket: {bucket_name}")
    
    # Check buffer size for performance monitoring
    image_buffer.seek(0, io.SEEK_END)
    buffer_size = image_buffer.tell()
    image_buffer.seek(0)  # Reset to beginning
    
    main_logger.info(f"Image buffer size: {buffer_size / 1024:.2f} KB")
    
    if buffer_size > 5 * 1024 * 1024:  # Log warning for images > 5MB
        main_logger.warning(f"Large image detected: {buffer_size / 1024 / 1024:.2f} MB")
    
    try:
        with log_sync_operation("s3_client_initialization", perf_logger):
            # Initialize S3 client
            aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
            aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")

            if aws_access_key_id and aws_secret_access_key:
                main_logger.debug("Using explicit AWS credentials")
                s3_client = boto3.client(
                    "s3",
                    aws_access_key_id=aws_access_key_id,
                    aws_secret_access_key=aws_secret_access_key,
                    region_name=aws_region,
                )
            else:
                main_logger.debug("Using IAM role credentials")
                s3_client = boto3.client("s3")

        # Generate unique ID and S3 path
        with log_sync_operation("path_generation", main_logger):
            image_id = generate_unique_image_id()
            s3_key = generate_s3_image_path(image_id)

        main_logger.info(f"Uploading to S3: {bucket_name}/{s3_key}")

        # Reset buffer position to beginning
        image_buffer.seek(0)

        # Upload to S3 without ACL (bucket must be configured for public access if needed)
        with log_sync_operation(f"s3_upload_{buffer_size}_bytes", perf_logger):
            s3_client.upload_fileobj(
                image_buffer,
                bucket_name,
                s3_key,
                ExtraArgs={
                    "ContentType": content_type,
                },
            )

        # Generate public URL
        public_url = f"https://{cloudfront_url}/{s3_key}"
        
        main_logger.info(f"S3 upload successful. Public URL: {public_url}")
        
        # Record performance metrics
        performance_metrics.record_metric("s3_upload_size", buffer_size / 1024, "KB")
        
        result = {
            "success": True,
            "bucket": bucket_name,
            "key": s3_key,
            "public_url": public_url,
            "message": "Image uploaded successfully to S3",
            "size_kb": buffer_size / 1024
        }
        
        main_logger.debug(f"S3 upload result: {result}")
        return result

    except Exception as e:
        error_msg = f"Failed to upload image to S3: {str(e)}"
        main_logger.error(error_msg)
        main_logger.error(f"Upload details - Bucket: {bucket_name}, Key: {s3_key if 's3_key' in locals() else 'N/A'}")
        main_logger.error(f"Buffer size: {buffer_size / 1024:.2f} KB, Content type: {content_type}")
        
        # Record error metrics
        performance_metrics.record_metric("s3_upload_errors", 1, "count")
        
        return {
            "success": False,
            "error": error_msg,
            "bucket": bucket_name,
            "key": s3_key if 's3_key' in locals() else None,
            "size_kb": buffer_size / 1024
        }
    