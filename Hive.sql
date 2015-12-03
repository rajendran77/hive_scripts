set hive.enforce.bucketing = true; 
set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=5120;

set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

create external table order_tmp
(
Order_ID String, 
Order_Date String, 
Order_Priority String, 
Cust_Name String, 
Order_Qty String, 
Unit_Price String, 
Sales_Amt float, 
Product_Container String, 
Ship_Date String
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/horton/order/data';



create table demo.orders
(
Order_ID String, 
Order_Priority String, 
Cust_Name String, 
Order_Qty String, 
Unit_Price String, 
Sales_Amt float, 
Product_Container String, 
Ship_Date String
)
PARTITIONED BY (Order_Date String)
CLUSTERED BY (Order_ID ) sorted by (ship_date) INTO 5 BUCKETS
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS ORC;

INSERT OVERWRITE TABLE  demo.orders
PARTITION  (Order_Date)
SELECT 
Order_ID,
Order_Priority ,
Cust_Name	,
Order_Qty ,
Unit_Price , 
Sales_Amt ,
Product_Container ,
Ship_Date ,
Order_Date
FROM order_tmp;

/*OVERWRITE TABLE */



create external table customer_tmp
(
first_name String, 
last_name String, 
address String, 
city String, 
country String,
postal String
)

ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/horton/data';


create table customer
(
first_name String, 
last_name String, 
address String, 
city String ,
postal String
)
PARTITIONED BY (country STRING)
CLUSTERED BY (postal) INTO 25 BUCKETS
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS ORC;

 
INSERT INTO CUSTOMER
PARTITION (country)
SELECT
first_name,
last_name,
address ,
city ,
country ,
postal
FROM customer_tmp;



INSERT INTO CUSTOMER
PARTITION (country='Hampshire') 
SELECT
first_name,
last_name,
company_name,
address ,
city ,
phone1 ,
phone2 ,
email ,
web 
FROM customer_tmp
where country='Hampshire';


LOAD DATA INPATH 'hdfs_file_or_directory_path' [OVERWRITE] INTO TABLE tablename
  [PARTITION (partcol1=val1, partcol2=val2 ...)]


LOAD DATA LOCAL INPATH '/home/rajendran/hive_files/customer.csv' OVERWRITE INTO TABLE customer PARTITION (county='Hampshire');


