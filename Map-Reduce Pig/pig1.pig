Customer_i = load '/user/hadoop/dataset1/Customer' using PigStorage(',') as (ID: int, name: chararray,A,G,C,S);
Transaction_i = load '/user/hadoop/dataset/Transaction' using PigStorage(',') as (I,CustID: int, Transtotal,TN,TD);

Customer = foreach Customer_i generate ID, name;
Transaction = foreach Transaction_i generate CustID, Transtotal;

TransInfo1 = join Transaction by CustID, Customer by ID using 'replicated';

TransInfo2 = group TransInfo1 by name;

Least_trans1 = foreach TransInfo2 generate group as name, COUNT(TransInfo1.Transtotal) as countT;

Least_trans2 = Group Least_trans1 All;

min = foreach Least_trans2 generate MIN(Least_trans1.countT) as mcount;
 
Least_trans3 = filter Least_trans1 by countT == min.mcount;

store Least_trans3 into '/user/hadoop/pig_output1';

