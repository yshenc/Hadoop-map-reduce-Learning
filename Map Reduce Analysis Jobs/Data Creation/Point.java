import java.io.*;
import java.util.Random;

/*public class Point {
    public static void main(String args[]) throws IOException{
        FileOutputStream fs = new FileOutputStream(new File("/users/yuchenshen/Desktop/Points.txt"));
        PrintStream p = new PrintStream(fs);
        int x = 1;
        while( x < 5000000 ) {
            double xlocation = (double) (Math.round((Math.random()*10000)*10)/10.0);
            double ylocation = (double) (Math.round((Math.random()*10000)*10)/10.0);
            p.print(xlocation+","+ylocation);
            x += 1;
            p.print("\n");
      }
   }
}*/ //slow



public class Point {

    public String Points() {
        int xlocation = (int)(Math.random()*10000+1);
        int ylocation = (int)(Math.random()*10000+1);
        return  xlocation + "," + ylocation;
    }

    public static void main(String[] Args) throws IOException {
        Point a = new Point();
        String fout = "/users/yuchenshen/Desktop/Points";
        FileOutputStream fos = new FileOutputStream(fout);
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));
        for (int i = 1; i <= 11000000; i++) {
            bw.write(a.Points());
            bw.newLine();
        }
        bw.close();
    }
}



