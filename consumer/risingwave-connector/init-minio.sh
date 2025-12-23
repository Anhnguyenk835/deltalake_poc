#!/bin/bash
set -e

MINIO_ENDPOINT="http://localhost:9000"
MINIO_ACCESS_KEY="admin"
MINIO_SECRET_KEY="password"
BUCKET_NAME="deltalake"

echo "================================================"
echo "MinIO Bucket Initialization"
echo "================================================"

# Check if MinIO is accessible
echo "Checking MinIO connectivity..."
if ! curl -s "${MINIO_ENDPOINT}/minio/health/live" > /dev/null; then
    echo "Error: MinIO is not accessible at ${MINIO_ENDPOINT}"
    echo "Make sure MinIO is running: docker-compose --profile risingwave up -d minio"
    exit 1
fi
echo "MinIO is accessible."

# Check if mc (MinIO Client) is installed
if command -v mc &> /dev/null; then
    echo "Using MinIO Client (mc) to create bucket..."

    # Configure mc alias
    mc alias set myminio "${MINIO_ENDPOINT}" "${MINIO_ACCESS_KEY}" "${MINIO_SECRET_KEY}" --api S3v4 2>/dev/null || true

    # Create bucket if it doesn't exist
    if mc ls myminio/${BUCKET_NAME} &> /dev/null; then
        echo "Bucket '${BUCKET_NAME}' already exists."
    else
        mc mb myminio/${BUCKET_NAME}
        echo "Bucket '${BUCKET_NAME}' created successfully."
    fi

    # List buckets
    echo ""
    echo "Available buckets:"
    mc ls myminio/

else
    echo "MinIO Client (mc) not found. Using Docker to create bucket..."

    # Use MinIO container to create bucket
    docker exec minio sh -c "
        mkdir -p /data/${BUCKET_NAME}
    " 2>/dev/null || {
        echo "Error: Could not create bucket via Docker."
        echo ""
        echo "Alternative: Create bucket manually via MinIO Console:"
        echo "  1. Open http://localhost:9001"
        echo "  2. Login with admin/password"
        echo "  3. Click 'Create Bucket' and name it '${BUCKET_NAME}'"
        exit 1
    }

    echo "Bucket '${BUCKET_NAME}' created successfully via Docker."
fi

echo ""
echo "================================================"
echo "MinIO Setup Complete!"
echo "================================================"
echo ""
echo "MinIO Console: http://localhost:9001"
echo "  Username: ${MINIO_ACCESS_KEY}"
echo "  Password: ${MINIO_SECRET_KEY}"
echo ""
echo "S3 Endpoint: ${MINIO_ENDPOINT}"
echo "Bucket: ${BUCKET_NAME}"
echo ""
echo "Next steps:"
echo "  1. Wait for RisingWave to be healthy"
echo "  2. Run: psql -h localhost -p 4566 -U root -d dev -f init-risingwave.sql"
echo "================================================"
