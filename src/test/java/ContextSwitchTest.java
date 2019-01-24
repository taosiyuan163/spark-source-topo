import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import com.google.common.collect.Maps;
import scala.Tuple1;
import scala.Tuple2;

public class ContextSwitchTest
{
    private static final long count = 1000000;

    public static void main(String[] args) throws Exception
    {
        Tuple2<ArrayList<Integer>, ConcurrentMap<Integer, Integer>> concurrency = concurrency();
        System.out.println("l="+concurrency._1
        .size()+"m="+concurrency._2.size());

//        serial();
    }


    private static Tuple2<ArrayList<Integer>,ConcurrentMap<Integer,Integer>> get(ArrayList<Integer> f ,ConcurrentMap<Integer,Integer> b){
        return new Tuple2<ArrayList<Integer>,ConcurrentMap<Integer,Integer>>(f,b);
    }

    private static Tuple2<ArrayList<Integer>,ConcurrentMap<Integer,Integer>> concurrency() throws Exception
    {

        final ArrayList<Integer> longs = new ArrayList<Integer>();

        final ConcurrentMap<Integer,Integer> f = new ConcurrentHashMap<Integer,Integer>();

        long start = System.currentTimeMillis();
        Thread thread = new Thread(new Runnable(){
            public void run()
            {
                int a = 0;
                for (int i = 0; i < count; i++)
                {
                    a += 1;
                    f.put(a, a);
                    longs.add(a);
                    System.out.println("a="+a+"---"+f.size()+"---"+longs.size());
                }
            }
        });
        thread.start();
//        int b = 0;
//        for (long i = 0; i < count; i++)
//        {
//            b --;
//        }
//        thread.join();
        long time = System.currentTimeMillis() - start;
        System.out.println("Concurrency：" + time);

        System.out.println("arraylist="+longs.size()+"map="+f.size());
        return get(longs,f);
    }

    private static void serial()
    {
        long start = System.currentTimeMillis();
        int a = 0;
        for (long i = 0; i < count; i++)
        {
            a += 1;
        }
        int b = 0;
        for (int i = 0; i < count; i++)
        {
            b --;
        }
        long time = System.currentTimeMillis() - start;
        System.out.println("Serial：" + time + "ms, b = " + b + ", a = " + a);
    }
}