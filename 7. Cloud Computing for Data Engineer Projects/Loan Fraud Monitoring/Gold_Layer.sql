create SCHEMA VIEW_LAYER;

--Number of Loans Applied from different Devices :
create view VW_LOAN_APPLICATIONS_BY_DEVICE as
select DEVICE_TYPE,count(*) as Count_per_Device
from BUSINESS.FACT_LOAN_APPLICATION fc
join BUSINESS.DIM_DEVICE dm
on fc.DEVICE_KEY=dm.DEVICE_KEY
group by DEVICE_TYPE
;
select * from VW_LOAN_APPLICATIONS_BY_DEVICE;


--Number of ACtive Loans PEr Customer :
Create view VW_CUSTOMER_ACTIVE_LOANS as
select ACTIVE_LOANS,fc.full_name
from  BUSINESS.DIM_CUSTOMER fc 
join BUSINESS.DIM_CREDIT_PROFILE dm
on fc.CUSTOMER_ID=dm.CUSTOMER_ID
;



--Fraudulent Loan Applications :
Create View VW_LOAN_FRAUD_DETECTION_SUMMARY as 
select count(*) as Number_of_Fraud_Cases from BUSINESS.FACT_LOAN_APPLICATION
where fraud_flag=1
 ;


-- Number of Risky Loan Applications :
Create View VW_RISKY_LOAN_APPLICATIONS as
select 
count(*) as Number_of_Risky_Loans
 from
 Business.fact_loan_application fc 
 left join  Business.dim_credit_profile dm 
 on dm.credit_key=fc.credit_key
 where credit_score_band='Risky'
 ;


-- Age Group of Customers Applying for Loans :
Create View VW_LOAN_APPLICATIONS_BY_AGE_GROUP as 
select Age_Groups,count(*) as Age_Group_Counts from 
(
select 
case when age>=18 and age<=30 then 'Early Starters'
 when age>=31 and age<=60 then 'Mid Rangers'
else 'Senior Citizens' end as Age_Groups
from Business.fact_loan_application fc
inner join business.dim_customer dm
on fc.customer_key=dm.customer_key
) as T Group by Age_Groups
;

