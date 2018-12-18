Customer_i = load '/user/hadoop/dataset1/Customer' using PigStorage(',') as (ID: int,n ,A,G,CountryCode: int,S);

Customer = foreach Customer_i generate ID, CountryCode;

CountryInfo = group Customer by CountryCode;

CI = foreach CountryInfo generate group, COUNT(Customer.ID) as cID;

Info = filter CI by cID > 5000 or cID < 2000;

store Info into '/user/hadoop/pig_output3';
