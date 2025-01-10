#docker run -d --name minio -p 9100:9000 -p 9101:9001 -e MINIO_ROOT_USER=admin -e MINIO_ROOT_PASSWORD=admin123 -v /home/ibrar/code/basic-common-crawl-pipeline/buckets:/data minio/minio server /data --console-address ":9001"
docker run -d --name minio -p 9100:9000 -p 9101:9001 -e MINIO_ROOT_USER=admin -e MINIO_ROOT_PASSWORD=admin123 minio/minio server /data --console-address ":9001"

export MINIO_ENDPOINT="http://localhost:9100"
export MINIO_ACCESS_KEY="admin"
export MINIO_SECRET_KEY="admin123"
