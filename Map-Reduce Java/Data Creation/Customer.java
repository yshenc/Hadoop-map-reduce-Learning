import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;

public class Customer {
    public static void main(String args[]) throws IOException{
        FileOutputStream fs = new FileOutputStream(new File("/home/hadoop/Desktop/Big_data_management/Customer.txt"));
        PrintStream p = new PrintStream(fs);
        int x = 1;
        while( x < 50001 ) {
            p.print(x+","); //print ID
          
            String chars = "abcdefghijklmnopqrstuvwxyz";
            int n = (int)(Math.random()*10) + 10;
            //System.out.print(n);
            int y = 0;
            while(y < n)  {
              p.print(chars.charAt((int)(Math.random() * 26)));
              y++;
            }//print Name

            p.print("," + (int)(Math.random() * 60 + 10)); //print Age

            String[] Gender = {"male", "female"}; 
            p.print("," + Gender[(int)(Math.random() * Gender.length)]);//print Gender

            p.print("," + (int)(Math.random() *10 +1 )); //print CountryCode

            p.print("," + (Math.random() * 9900 + 100)); //print Salary

            x++;
            p.print("\n");
      }
   }
}
