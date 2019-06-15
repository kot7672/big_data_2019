/*
Нужно написать две программы:
Первая генерирует бинарный файл (min 2Гб), состоящий из случайных 32-рязрядных беззнаковых целых чисел (big endian).
Вторая считает сумму этих чисел (с применением длинной арифметики), находит минимальное и максимальное число.

Реализуйте две версии:
1. Простое последовательное чтение
2. Многопоточная + memory-mapped files. Сравните время работы.
*/

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.math.BigInteger;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class Main {

    private static void generateData(long limit) {
        Random rand = new Random();
        long currentSize = 0L;
        try(FileWriter writer = new FileWriter("data.txt", false))
        {
            while(currentSize < limit) {
                BigInteger randInt = new BigInteger(32, rand);
                String str = randInt.toString();
                writer.append(str);
                writer.append('\n');
                currentSize += (str.length()+1);
            }
            BigInteger randInt = new BigInteger(32, rand);
            writer.append(randInt.toString());
        }
        catch (IOException ex) {
            System.out.println(ex.getMessage());
        }
    }

    private static BigInteger[] simpleReadData() {
        BigInteger min = new BigInteger("4294967297");
        BigInteger max = BigInteger.valueOf(-1);
        BigInteger sum = BigInteger.valueOf(0);
        try(BufferedReader reader = new BufferedReader(new FileReader("data.txt")))
        {
            String line;
            while ((line = reader.readLine()) != null) {
                BigInteger num = new BigInteger(line);
                if(num.compareTo(min) == -1) {
                    min = num;
                }
                if(num.compareTo(max) == 1) {
                    max = num;
                }
                sum = sum.add(num);
            }
        }
        catch (IOException ex) {
            System.out.println(ex.getMessage());
        }
        return new BigInteger[] {min, max, sum};
    }

    static class multiprocess {

        private BigInteger min = new BigInteger("4294967297");
        private BigInteger max = BigInteger.valueOf(-1);
        private BigInteger sum = BigInteger.valueOf(0);
        private BlockingQueue<String> bq = new ArrayBlockingQueue<>(10000);

        final Object lock = new Object();

        class dataReader extends Thread {
            @Override
            public void run() {
                try(BufferedReader reader = new BufferedReader(new FileReader("data.txt")))
                {
                    String line;
                    while ((line = reader.readLine()) != null) bq.put(line);
                    for(int i=0; i<6; i++) bq.put("STOP");
                }
                catch (Exception ex) {
                    System.out.println(ex.getMessage());
                }
            }
        }

        class calculatingThread extends Thread {
            @Override
            public void run() {
                try {
                    BigInteger currentMin = new BigInteger("4294967297");
                    BigInteger currentMax = BigInteger.valueOf(-1);
                    BigInteger currentSum = BigInteger.valueOf(0);
                    while (true) {
                        String data = bq.take();
                        if (data.equals("STOP")) break;
                        BigInteger num = new BigInteger(data);
                        if (currentMin.compareTo(num) == 1) currentMin = num;
                        if (currentMax.compareTo(num) == -1) currentMax = num;
                        currentSum = currentSum.add(num);
                    }
                    synchronized (lock) {
                        if(currentMax.compareTo(max)==1) max = currentMax;
                        if(currentMin.compareTo(min)==-1) min = currentMin;
                        sum = sum.add(currentSum);
                    }
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }

        public BigInteger[] calculate() {
            (new dataReader()).start();
            calculatingThread[] threads = new calculatingThread[6];
            for(int i=0;i<6;i++) {
                threads[i] = new calculatingThread();
                threads[i].start();
            }
            for(calculatingThread thread: threads) {
                try {
                    thread.join();
                }
                catch (InterruptedException e) {
                    thread.interrupt();
                }
            }
            return new BigInteger[] {min, max, sum};
        }
    }

    public static void main(String[] args) {
        System.out.println("Generating data");
        long time = System.currentTimeMillis();
        //generateData(2147483648L);
        System.out.println("Time: " + (System.currentTimeMillis()-time) + "\n");
        System.out.println("Simple method");
        time = System.currentTimeMillis();
        BigInteger[] data = simpleReadData();
        System.out.println("min: " + data[0].toString() + ", max: " + data[1].toString() + ", sum: " + data[2]);
        System.out.println("Time: " + (System.currentTimeMillis()-time) + "\n");
        System.out.println("Multithread");
        time = System.currentTimeMillis();
        data = (new multiprocess()).calculate();
        System.out.println("min: " + data[0].toString() + ", max: " + data[1].toString() + ", sum: " + data[2]);
        System.out.println("Time: " + (System.currentTimeMillis()-time) + "\n");
    }
}
