# Loan Fraud Monitoring 

# Scenario :
- Banks gets the Applications for various types of loans(home,vehicle,personal,etc).
- Banks get this data at a realtime basis and they need to analyze the how many of the request are genuine and how many are fraud.
- Then create a report and present it to higher management for business actions.

# Architecture :

![alt text](image-6.png)

# Data Model :

![Data Model Loan Fraud.png](<Data Model Loan Fraud.png>)


# Data Flow :

# - KQL Database -> Fabric Database(RAW) : Dataflow automated by Data Pipeline.
![alt text](image.png)


# Credit Info : Daily Load from External csv file stored in Lakehouse : use of copy data to load CIBIL Data.
![alt text](image-1.png)
# - Fabric Database(RAW) -> Fabric Database(SILVER) : Scripts using Data Pipeline.
![alt text](image-3.png)
# - Fabric Database(SILVER) -> Fabric Database(GOLD) : View Layer .
![alt text](image-2.png)

# POWER BI REPORT
![alt text](image-5.png)