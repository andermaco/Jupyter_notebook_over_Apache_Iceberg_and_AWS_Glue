#!/bin/bash

# libraries path wherever jar files will be downloaded to
DIR="`pwd`/deploy/jar_libraries" 

URLS=(
  "https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.17.161/bundle-2.17.161.jar"  
  "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.7.1/iceberg-spark-runtime-3.5_2.12-1.7.1.jar"
  "https://repo1.maven.org/maven2/software/amazon/awssdk/url-connection-client/2.17.161/url-connection-client-2.17.161.jar"
)

# Check directory exits, if not create it.
mkdir -p "$DIR"

for URL in "${URLS[@]}"; do
  FILENAME=$(basename "$URL")  
  wget -q "$URL" -O "$DIR/$FILENAME"
  echo "Download: $FILENAME"
done