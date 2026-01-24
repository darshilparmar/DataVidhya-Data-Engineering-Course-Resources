
#Installing Java 
sudo yum -y install java-11

#Installing Python pip
sudo yum -y install python-pip

#Installing Kafka 
wget https://archive.apache.org/dist/kafka/2.8.1/kafka_2.12-2.8.1.tgz

#Unzipping the Kafka File 
tar -xzf kafka_2.12-2.8.1.tgz

#Providing the Access to execute the shell scripts present in the Kafka Folder.
chmod +x kafka_2.12-2.8.1/bin/*.sh

#Downloading the library for AWS IAM MSK Setup Java JAR.(We have to run it in kafka_2.12-2.8.1/libs/)
wget https://github.com/aws/aws-msk-iam-auth/releases/download/v2.3.0/aws-msk-iam-auth-2.3.0-all.jar


#Creation of the client.properties file in (kafka_2.12-2.8.1/bin/):

security.protocol=SASL_SSL
sasl.mechanism=AWS_MSK_IAM
sasl.jaas.config=software.amazon.msk.auth.iam.IAMLoginModule required;
sasl.client.callback.handler.class=software.amazon.msk.auth.iam.IAMClientCallbackHandler

#Export CLASSPATH
export CLASSPATH=/home/ec2-user/kafka_2.12-2.8.1/libs/aws-msk-iam-auth-2.3.0-all.jar 


#Essentials Python Packages to install 
pip install kafka-python
pip install aws-msk-iam-sasl-signer-python

#Attach IAM Policy to Ec2 Instance for MSK Access.
