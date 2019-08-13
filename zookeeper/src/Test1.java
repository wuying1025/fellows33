import java.util.Arrays;
import java.util.Scanner;

public class Test1 {
    public static void main(String[] args) {
        Scanner sc = new Scanner(System.in);
        String count = sc.nextLine();//个数和操作次数
        String nums = sc.nextLine();//数组
        String[] split = count.split(" ");
        int[] num = new int[Integer.parseInt(split[1])];
        for(int i=0;i<Integer.parseInt(split[1]);i++){
            num[i] = Integer.parseInt(sc.nextLine());
        }
        int[] a1 = StringToInt(nums.split(" "));
        Arrays.sort(a1);//已有数组   num是操作数
        for(int i=0;i<num.length;i++){
            int index = getIndex(a1,num[i]);
            if(index != -1){
                System.out.println(a1.length - index);
                for(int j=index;j<a1.length;j++){
                    a1[j] = a1[j] -1;
                }
                Arrays.sort(a1);
            }else if(num[i]<a1[0]){
                System.out.println(a1.length);
                for(int j=0;j<a1.length;j++){
                    a1[j] = a1[j] -1;
                }
            }else{
                System.out.println(0);
            }
        }
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
