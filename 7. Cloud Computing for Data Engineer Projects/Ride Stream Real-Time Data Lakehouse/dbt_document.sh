#DBT Docs Generate and checking on the documents 

dbt docs generate 
dbt docs serve --host 0.0.0.0 --port 8080

http://<EC2_PUBLIC_IP>:8080

#Process to restart the docs serve 

1)Kill the previous process that is running 
-lsof -i :8080
-kill -9 <PID>
