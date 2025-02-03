echo "" >> $SPARK_CONF_DIR/spark-defaults.conf
echo "spark.sql.extensions    org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions" >> $SPARK_CONF_DIR/spark-defaults.conf
echo "spark.driver.memory    12g" >> $SPARK_CONF_DIR/spark-defaults.conf

cp deploy/jar_libraries/iceberg-spark-runtime-3.3_2.12-1.4.2.jar /home/glue_user/spark/jars/
#cp deploy/jar_libraries/iceberg-spark-runtime-3.5_2.13-1.7.1.jar /home/glue_user/spark/jars/

cp deploy/jar_libraries/bundle-2.17.161.jar /home/glue_user/spark/jars/
#cp deploy/jar_libraries/url-connection-client-2.17.161.jar /home/glue_user/spark/jars/
# rm /home/glue_user/aws-glue-libs/jars/utils-2.15.32.jar
