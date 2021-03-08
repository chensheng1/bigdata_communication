package hbase;

import java.util.HashSet;
import java.util.Scanner;

/**
 * Created by asus on 2020/8/27.
 */
public class test {

    public static void main(String[] args) throws Exception {
        Scanner in = new Scanner(System.in);
        int n = in.nextInt();
        int[] fa=new int[n];
        int[] value = new int[n];
        for(int i=0;i<n;i++){
            fa[i]=in.nextInt();
        }
        for(int j=0;j<n;j++){
            value[j]=in.nextInt();
        }
        HashSet<Integer> set=new HashSet<Integer>();
        for(int i=0;i<=value[0];i++){
            set.add(i*value[0]);
        }

    }
}
