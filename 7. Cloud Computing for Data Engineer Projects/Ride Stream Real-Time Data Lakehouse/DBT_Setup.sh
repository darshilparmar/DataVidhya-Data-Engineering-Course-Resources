#Login into AWS CLI
aws configure

#Creation of virtual ENV :

python3 -m venv dbt-env

#Activate the Virtual ENV

source dbt-env/bin/activate

#Install Necessary Packages

pip install dbt-core dbt-athena

#Install Git in EC2

sudo yum install git

#Creation of Dbt project we need to run command and place the details there.
dbt init

#Creation of Connection : You need to go to the folder where the project.yml folder is there then run this 

dbt debug

#Checking the connection folder do cd.. then go to .dbt folder and do ls

vi profiles.yml


