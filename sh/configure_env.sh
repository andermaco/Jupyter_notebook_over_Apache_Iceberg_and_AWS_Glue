# Update pip
pip install --upgrade pip

# Install wrangler
pip install numpy==1.26.4
pip install awswrangler

# Configure spark
echo "" >> $SPARK_CONF_DIR/spark-defaults.conf
echo "spark.sql.extensions    org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions" >> $SPARK_CONF_DIR/spark-defaults.conf
echo "spark.driver.memory    12g" >> $SPARK_CONF_DIR/spark-defaults.conf

# Copy libs to docker image env
cp ./deploy/jar_libraries/iceberg-spark-runtime-3.3_2.12-1.6.1.jar /home/glue_user/spark/jars/
cp ./deploy/jar_libraries/bundle-2.17.161.jar /home/glue_user/spark/jars/
cp ./deploy/jar_libraries/url-connection-client-2.17.161.jar /home/glue_user/spark/jars/

