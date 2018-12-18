import java.io.*;
import java.io.PrintStream;

public class Rectangle {


    public String Rectangle(int n) {
        int bottomL_x = (int)(Math.random()*10000);
        int bottomL_y = (int)(Math.random()*10000);
        int height = (int)(Math.random()*20+1);
        int Width = (int)(Math.random()*5+1);
        return "r" + n + "," + bottomL_x + "," + bottomL_y +","+height + "," + Width;
    }
    
    public static void main(String[] Args) throws IOException {
        Rectangle a = new Rectangle();

        String fout = "/users/yuchenshen/Desktop/rectangles";
        FileOutputStream fos = new FileOutputStream(fout);
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));
        for (int n = 1; n <= 4000000; n++) {
            bw.write(a.Rectangle(n));
            bw.newLine();
        }
        bw.close();
    }
}