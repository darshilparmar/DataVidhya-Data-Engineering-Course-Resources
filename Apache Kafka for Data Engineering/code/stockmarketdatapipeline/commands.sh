# Create virtual environment
python3 -m venv venv

# Activate virtual environment
source venv/bin/activate  # On Unix/macOS
# OR
venv\Scripts\activate     # On Windows

docker-compose exec kafka kafka-console-consumer \
    --bootstrap-server localhost:9092 \
    --topic continuous-stock-data-producer \
    --from-beginning

docker-compose exec kafka kafka-topics --list --bootstrap-server localhost:9092


#spark jars
"org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.11.901"

docker exec stockmarketdatapipeline_spark-master_1 \
      spark-submit \
        --master spark://spark-master:7077 \
        --packages org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.11.901 \
        /opt/spark/jobs/spark_batch_processor.py {{ ds }}