import java.text.NumberFormat;
import java.util.Arrays;
import java.util.Scanner;

public class Test {
    public static void main(String[] args) {
        Scanner sc = new Scanner(System.in);
        int num = Integer.parseInt(sc.nextLine());
        String scores = sc.nextLine();
        int count = Integer.parseInt(sc.nextLine());
        int[] checks = new int[count];
        for(int i=0;i<count;i++){
            checks[i] = Integer.parseInt(sc.nextLine());
        }
        String[] splits = scores.split(" ");
        int[] a1 = StringToInt(splits);
        int[] a2 = a1.clone();
        Arrays.sort(a2);
        for(int i=0;i<checks.length;i++){
            int s = a1[checks[i]-1];
            int index = getIndex(a2, s);
            String result = percent(index,num);
            System.out.println(result.substring(0,result.length()-1));
        }
    }

    public static String percent(double p1,double p2){
        String str;
        double p3 = p1/p2;
        NumberFormat nf  =  NumberFormat.getPercentInstance();
        nf.setMinimumFractionDigits(6);
        str = nf.format(p3);
        return  str;
    }

    public static int getIndex(int[] arr, int value) {
        for (int i = 0; i < arr.length; i++) {
            if (arr[i] == value) {
                return i;
            }
        }
        return -1;
    }
    public static int[] StringToInt(String[] arrs){
        int[] ints = new int[arrs.length];
        for(int i=0;i<arrs.length;i++){
            ints[i] = Integer.parseInt(arrs[i]);
        }
        return ints;
    }

}
