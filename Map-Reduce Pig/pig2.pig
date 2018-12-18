Customer_i = load '/user/hadoop/dataset1/Customer' using PigStorage(',') as (ID: int, name: chararray,A,G,C,Salary);
Transaction_i = load '/user/hadoop/dataset/Transaction' using PigStorage(',') as (I,CustID: int, Transtotal,NumItem: int,TD);

Customer = foreach Customer_i generate ID, name, Salary;
Transaction = foreach Transaction_i generate CustID, Transtotal, NumItem;

TransInfo1 = join Transaction by CustID, Customer by ID using 'replicated';

TransInfo2 = group TransInfo1 by (CustID, name, Salary);

transInfo = foreach TransInfo2 generate group.CustID, group.name, group.Salary, COUNT(TransInfo1.Transtotal), SUM(TransInfo1.Transtotal), MIN(TransInfo1.NumItem);

store transInfo into '/user/hadoop/pig_output2';
