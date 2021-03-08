package hbase;

import java.util.Scanner;

/**
 * Created by asus on 2020/9/5.
 */
public class test11 {

    public static void main(String[]  arg){
        Scanner in = new Scanner(System.in);
        String str1 = in.nextLine();
        char[] ch = str1.toCharArray();
        for(int i=0;i<str1.length();i++){
            if(ch[i]>='a' && ch[i]<='z'){
                if(str1.charAt(i)=='z'){
                    ch[i]='A';
                }else {
                    ch[i]=(char)(str1.charAt(i) + 33);
                }
            }else if(str1.charAt(i)>='A' && str1.charAt(i)<='B'){
                if(str1.charAt(i)=='Z'){
                    ch[i]='a';
                }else {
                    ch[i]=(char)(str1.charAt(i) - 31);
                }
            }else{
                if(str1.charAt(i)=='9'){
                    ch[i]='0';
                }else {
                    ch[i]=(char)(str1.charAt(i) + 1);
                }
            }
        }
        System.out.println( String.valueOf(ch));
    }

}
