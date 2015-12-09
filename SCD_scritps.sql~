/*https://svn.apache.org/repos/asf/hive/trunk/contrib/src/java/org/apache/hadoop/hive/contrib/udf/UDFRowSequence.java*/
/*http://hadooptutorial.info/writing-custom-udf-in-hive-auto-increment-column-hive/*/
/*http://www.hadooptpoint.com/how-to-write-hive-udf-example-in-java/*/

/*hive-env.xml
export HIVE_AUX_JARS_PATH="/home/rajendran/hive_scripts/HiveSequenceUDF.jar" */


--drop table SCD.CUSTOMER;
create table SCD.CUSTOMER
(

            CUST_NO       VARCHAR(50) ,
            FIRSTNAME    VARCHAR(100) ,
            LASTNAME     VARCHAR(100) ,
            DOB    DATE ,
            ADDRESS        VARCHAR(100) ,
            STREET           VARCHAR(100) ,
            CITY VARCHAR(100) ,
            COUNTRY VARCHAR(100) ,
            POSTCODE VARCHAR(100) ,
            PHONE INT
)
CLUSTERED BY(cust_no) INTO 5 BUCKETS
STORED as orc TBLPROPERTIES ('transactional'='true');

INSERT INTO TABLE SCD.CUSTOMER VALUES('101','Raj','Veerappan','1977-01-01','42','Solent Road','Portsmouth','UK','PO6 1HJ',5678) ;
INSERT INTO TABLE SCD.CUSTOMER VALUES('102','Kanna','Natarsan','1977-01-01','42','Karavalasu','Erode','India','631 312',1234) ;

INSERT INTO TABLE SCD.CUSTOMER VALUES('101','Rajendran','Veerappan','1977-01-01','42','Solent Road','Portsmouth','UK','PO6 1HJ',5678) ;
INSERT INTO TABLE SCD.CUSTOMER VALUES('103','Muthu','Krishnan','1977-01-01','42','Solent Road','Portsmouth','UK','PO6 1HJ',5678) ;

 
 
--drop table SCD.TMP_DIM_CUSTOMER;
create table SCD.TMP_DIM_CUSTOMER 
(

            SURROGATE_KEY    INT ,
            CUST_NO       VARCHAR(50) ,
            FIRSTNAME    VARCHAR(100) ,
            LASTNAME     VARCHAR(100) ,
            DOB    DATE ,
            ADDRESS        VARCHAR(100) ,
            STREET           VARCHAR(100) ,
            CITY    VARCHAR(100) ,
            COUNTRY       VARCHAR(100) ,
            POSTCODE     VARCHAR(100) ,
            PHONE INT,
            START_DT      String,
            END_DT        String,
            IND_UPDATE    varchar(1)
)
CLUSTERED BY(cust_no) INTO 5 BUCKETS
STORED as orc TBLPROPERTIES ('transactional'='true');

--drop table SCD.DIM_CUSTOMER;
create table SCD.DIM_CUSTOMER 
(

            SURROGATE_KEY    INT ,
            CUST_NO       VARCHAR(50) ,
            FIRSTNAME    VARCHAR(100) ,
            LASTNAME     VARCHAR(100) ,
            DOB    DATE ,
            ADDRESS        VARCHAR(100) ,
            STREET           VARCHAR(100) ,
            CITY    VARCHAR(100) ,
            COUNTRY       VARCHAR(100) ,
            POSTCODE     VARCHAR(100) ,
            PHONE INT,
            START_DT      String,
            END_DT        String
) 

CLUSTERED BY(cust_no) INTO 5 BUCKETS
STORED as orc TBLPROPERTIES ('transactional'='true');

insert  into table SCD.TMP_DIM_CUSTOMER 
select * from (
 select 
 incr() SURROGATE_KEY,
 CUSTOMER.CUST_NO CUST_NO,
 CUSTOMER.FIRSTNAME FIRSTNAME,
 CUSTOMER.LASTNAME LASTNAME,
 CUSTOMER.DOB DOB,
 CUSTOMER.ADDRESS ADDRESS,
 CUSTOMER.STREET STREET,
 CUSTOMER.CITY CITY,
 CUSTOMER.COUNTRY COUNTRY,
 CUSTOMER.POSTCODE POSTCODE,
 CUSTOMER.PHONE PHONE,
 from_unixtime(unix_timestamp(),'y-M-d') START_DT,
 to_date("1999-01-01 00:00:00") END_DT,
 'I' IND_UPDATE
 from SCD.CUSTOMER CUSTOMER
 where (1=1)
) S
where NOT EXISTS (
 select 'x'
 from SCD.DIM_CUSTOMER T
 where T.CUST_NO = S.CUST_NO
 and ((S.FIRSTNAME = T.FIRSTNAME))
  and ((S.LASTNAME = T.LASTNAME) )
  and ((S.DOB = T.DOB) )
  and ((S.ADDRESS = T.ADDRESS) )
  and ((S.STREET = T.STREET) )
  and ((S.CITY = T.CITY) )
  and ((S.COUNTRY = T.COUNTRY))
  and ((S.POSTCODE = T.POSTCODE))
  and ((S.PHONE = T.PHONE))
 and END_DT  =   to_date("1999-01-01 00:00:00")  
);



-- reflect("java.util.UUID", "randomUUID")


update SCD.TMP_DIM_CUSTOMER 
set IND_UPDATE = 'U'
where  CUST_NO
    IN (
     select   T.CUST_NO
      from    SCD.DIM_CUSTOMER T JOIN SCD.TMP_DIM_CUSTOMER S
      ON       T.CUST_NO = S.CUST_NO
      and      T.END_DT        =  to_date("1999-01-01 00:00:00")
      and      (S.FIRSTNAME = T.FIRSTNAME) 
      and      (S.LASTNAME = T.LASTNAME)
      and      (S.DOB = T.DOB)
      and      (S.ADDRESS = T.ADDRESS)
      and      (S.STREET = T.STREET)
      and      (S.CITY = T.CITY)
      and      (S.COUNTRY = T.COUNTRY)
      and      (S.POSTCODE = T.POSTCODE)
      and      (S.PHONE = T.PHONE)
      );

 

update SCD.TMP_DIM_CUSTOMER  
set  IND_UPDATE = 'U'
where CUST_NO 
      in (
      select   T.CUST_NO
      from    SCD.DIM_CUSTOMER T JOIN SCD.TMP_DIM_CUSTOMER S
      where    T.CUST_NO = S.CUST_NO
      and      T.END_DT        =  to_date("1999-01-01 00:00:00")
      and      ((S.FIRSTNAME is  null) and (T.FIRSTNAME is null)) 
      and      ((S.LASTNAME is  null) and (T.LASTNAME is null)) 
      and      ((S.DOB is  null) and (T.DOB is null) )
      and      ((S.ADDRESS is  null) and (T.ADDRESS is null)) 
      and      ((S.STREET is  null) and (T.STREET is null)) 
      and      ((S.CITY is  null) and (T.CITY is null) )
      and      ((S.COUNTRY is  null) and (T.COUNTRY is null) )
      and      ((S.POSTCODE is  null) and (T.POSTCODE is null))
      and      ((S.PHONE is  null) and (T.PHONE is null) )
      );
-----
/*
update SCD.DIM_CUSTOMER
set END_DT =  from_unixtime(unix_timestamp(),'y-M-d') 
where CUST_NO IN (
      select X.CUST_NO
      from SCD.TMP_DIM_CUSTOMER X
      where  X.IND_UPDATE = 'I')
and END_DT = to_date("1999-01-01 00:00:00");


*/
update SCD.DIM_CUSTOMER
set END_DT = (
      select  
      S.START_DT
      from SCD.TMP_DIM_CUSTOMER S JOIN SCD.DIM_CUSTOMER T
      where  S.CUST_NO  = T.CUST_NO
      and S.IND_UPDATE = 'I')
where CUST_NO IN (
      select X.CUST_NO
      from SCD.TMP_DIM_CUSTOMER X
      where  X.IND_UPDATE = 'I')
and END_DT  =   to_date("1999-01-01 00:00:00"); 

 -------
insert into  SCD.DIM_CUSTOMER
select  
 SURROGATE_KEY,
 CUST_NO,
 FIRSTNAME,
 LASTNAME,
 DOB,
 ADDRESS,
 STREET,
 CITY,
 COUNTRY,
 POSTCODE,
 PHONE,
 START_DT,
 END_DT 
from    SCD.TMP_DIM_CUSTOMER S
where S.IND_UPDATE = 'I';

truncate table tmp_dim_customer;

-----------

select * from customer;
101	Raj	Veerappan	1977-01-01	42	Solent Road	Portsmouth	UK	PO6 1HJ	5678
102	Kanna	Natarsan	1977-01-01	42	Karavalasu	Erode	India	631 312	1234

select * from dim_customer;



