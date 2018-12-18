import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;

public class Transaction {
    public static void main(String args[]) throws IOException{
        FileOutputStream fs = new FileOutputStream(new File("/home/hadoop/Desktop/Big_data_management/Transaction.txt"));
        PrintStream p = new PrintStream(fs);
        int x = 1;
        while( x < 5000001 ) {
            p.print(x+","); //print TransID
            
            p.print((int)(Math.random()*50000+1));//print CustID
            
            p.print("," + (Math.random() * 990 + 10)); //print TransTotal
            
            p.print("," + (int)(Math.random() *10 +1 ) + ","); //print TransNumItems
            
            String chars = "abcdefghijklmnopqrstuvwxyz  ";
            int n = (int)(Math.random()*30) + 20;
            int y = 0;
            while(y < n)  {
              p.print(chars.charAt((int)(Math.random() * 28)));
              y++;
            }//print TransDesc

            x++;
            p.print("\n");
      }
   }
}
