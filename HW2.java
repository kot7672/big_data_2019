/*
1. Нужно сгенерировать файл, содержащий 2000 128-битных случайных целых чисел, каждое число на отдельной строке.

2. Посчитать, какое суммарное количество простых множителей присутствует при факторизации всех чисел.  Например, пусть всего два числа: 6 и 8.
6 = 2 * 3, 8 = 2 * 2 * 2. Ответ 5.  При реализации нужно использовать операции с длинной арифметикой (*BigInteger* и т.д.)

3. Реализовать подсчет
    - простым последовательным алгоритмом
    - многопоточно, с использованием примитивов синхронизации
    - с помощью Akka (или аналога)
    - c помощью RxJava (или аналога)

4. Измерить время выполнения для каждого случая. Использовать уровень параллельности в соответствии с числом ядер вашего CPU.
 */

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import io.reactivex.Flowable;
import io.reactivex.schedulers.Schedulers;
import scala.concurrent.Await;
import scala.concurrent.duration.Duration;

import java.io.*;
import java.math.*;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class Main {

    private static void generateData(int limit, int bits) {
        Random rand = new Random();
        try(FileWriter writer = new FileWriter("data.txt", false))
        {
            for(int i=1; i<limit; i++) {
                BigInteger randInt = new BigInteger(bits, rand);
                writer.append(randInt.toString());
                writer.append('\n');
            }
            BigInteger randInt = new BigInteger(bits, rand);
            writer.append(randInt.toString());
        }
        catch (IOException ex) {
            System.out.println(ex.getMessage());
        }
    }

    private static BigInteger countMultipliers(BigInteger num) {
        BigInteger currentNum = num;
        BigInteger limit = currentNum.divide(BigInteger.valueOf(2));
        BigInteger currentMultiplier = BigInteger.valueOf(2);
        BigInteger multipliers = BigInteger.ZERO;
        while(true) {
            if(currentNum.mod(currentMultiplier).equals(BigInteger.ZERO)) {
                multipliers = multipliers.add(BigInteger.ONE);
                currentNum = currentNum.divide(currentMultiplier);
                if(currentNum.equals(BigInteger.ONE)) break;
            }
            else {
                currentMultiplier = currentMultiplier.add(BigInteger.ONE);
                if(currentMultiplier.compareTo(limit)>0) {
                    multipliers = multipliers.add(BigInteger.ONE);
                    break;
                }
            }

        }
        return multipliers;
    }

    private static BigInteger simpleMethod() {
        BigInteger multipliers = BigInteger.ZERO;
        try {
            FileInputStream fileStream = new FileInputStream("data.txt");
            BufferedReader br = new BufferedReader(new InputStreamReader((fileStream)));
            String line;
            while((line = br.readLine())!=null) {
                multipliers = multipliers.add(countMultipliers(new BigInteger(line)));
            }
        }
        catch (IOException e) {
            System.out.println(e.getMessage());
        }
        return multipliers;
    }

    public static class multiThreadMethod {

        private BlockingQueue<BigInteger> bq;
        private BigInteger totalMultipliers = BigInteger.ZERO;
        private final Object lock = new Object();

        private class countingThread extends Thread {
            @Override
            public void run() {
                while(true) {
                    BigInteger num = bq.poll();
                    if (num == null) break;
                    BigInteger multipliers = countMultipliers(num);
                    synchronized (lock) {
                        totalMultipliers = totalMultipliers.add(multipliers);
                    }
                }
            }
        }

        ArrayBlockingQueue<BigInteger> readData(int limit) {
            ArrayBlockingQueue<BigInteger> bq = new ArrayBlockingQueue<>(limit);
            try {
                FileInputStream fileStream = new FileInputStream("data.txt");
                BufferedReader br = new BufferedReader(new InputStreamReader((fileStream)));
                String line;
                while((line = br.readLine())!=null) {
                    bq.put(new BigInteger(line));
                }
            }
            catch (Exception e) {
                System.out.println(e.getMessage());
            }
            return bq;
        }

        BigInteger count(int threads, int limit) {
            bq = readData(limit);
            countingThread[] counters = new countingThread[threads];
            for (int i=0; i < threads; i++) {
                counters[i] = new countingThread();
                counters[i].start();
            }
            for (int thread=0; thread < threads; thread++) {
                try {
                    counters[thread].join();
                }
                catch (InterruptedException e) {
                    counters[thread].interrupt();
                }
            }
            return totalMultipliers;
        }

    }

    public static class akka {

        ActorSystem system;
        ActorRef main;
        BigInteger counter;
        ArrayBlockingQueue<BigInteger> bq;

        private class mainActor extends AbstractActor {

            int activeWorkers;
            int totalWorkers;
            long startTime;
            ActorRef[] workersList;

            mainActor() {}

            @Override
            public Receive createReceive() {
                return receiveBuilder()
                        .match(Integer.class, workers -> {
                            startTime = System.currentTimeMillis();
                            activeWorkers = workers;
                            totalWorkers = workers;
                            workersList = new ActorRef[workers];
                            for (int worker=0; worker < workers; worker++) {
                                workersList[worker] = system.actorOf(Props.create(countingActor.class, () -> new countingActor()));
                                BigInteger num = bq.poll();
                                workersList[worker].tell(num, getSelf());
                            }
                        })
                        .match(BigInteger.class, i -> {
                            counter = counter.add(i);
                            BigInteger num = bq.poll();
                            if (num == null) {
                                activeWorkers -= 1;
                                if (activeWorkers == 0) system.terminate();
                            }
                            else {
                                getSender().tell(num, getSelf());
                            }
                        })
                        .build();
            }
        }

        private class countingActor extends AbstractActor {

            countingActor() {}

            @Override
            public Receive createReceive() {
                return receiveBuilder()
                        .match(BigInteger.class, i -> {
                            getSender().tell(countMultipliers(i), self());
                        })
                        .build();
            }
        }

        BigInteger count(int workers, int limit) {
            system = ActorSystem.create("factorization-akka");
            main = system.actorOf(Props.create(mainActor.class, () -> new mainActor()));
            counter = BigInteger.ZERO;

            bq = new ArrayBlockingQueue<>(limit);
            try {
                FileInputStream fileStream = new FileInputStream("data.txt");
                String line;
                BufferedReader br = new BufferedReader(new InputStreamReader((fileStream)));

                while((line = br.readLine())!=null) {
                    bq.put(new BigInteger(line));
                }
            }
            catch (Exception e) {
                System.out.println(e.getMessage());
            }

            main.tell(workers, main);
            try {
                Await.result(system.whenTerminated(), Duration.create(5, TimeUnit.HOURS));
            }
            catch (Exception e) {
                System.out.println(e.getMessage());
            }
            return counter;
        }
    }

    public static class rxjava {

        BigInteger counter = BigInteger.ZERO;
        String[] data;

        void updateCounter(BigInteger value) {
            counter = counter.add(value);
        }

        void loadData() {
            try {
                FileInputStream fileStream = new FileInputStream("data.txt");
                BufferedReader br = new BufferedReader(new InputStreamReader((fileStream)));
                String line;
                ArrayList<String> d = new ArrayList<>();
                while((line = br.readLine())!=null) {
                    d.add(line);
                }
                data = d.toArray(new String[0]);
            }
            catch (IOException e) {
                System.out.println(e.getMessage());
            }
        }

        BigInteger count() {
            loadData();
            Flowable.fromArray(data)
                    .parallel()
                    .runOn(Schedulers.computation())
                    .map( i -> countMultipliers(new BigInteger(i)))
                    .sequential()
                    .blockingSubscribe(this::updateCounter);
            return counter;
        }
    }

    public static void runMethod(String method, int threads, int limit) {
        BigInteger result;
        long time = System.currentTimeMillis();
        switch (method) {
            case "simple":
                System.out.println("Simple method");
                result = simpleMethod();
                break;
            case "multithread":
                System.out.println("Multithread");
                result = (new multiThreadMethod().count(threads, limit));
                break;
            case "akka":
                System.out.println("Akka");
                result = (new akka().count(threads, limit));
                break;
            case "rxjava":
                System.out.println("RXJava");
                result = new rxjava().count();
                break;
            default:
                System.out.println("Unknown method");
                return;
        }
        time = System.currentTimeMillis() - time;
        System.out.println("Result: " + result);
        System.out.println("Time: " + time + "\n");
    }

    public static void main(String[] args) {

        int limit = 2000;
        int bits = 32;
        int threads = 6;

        System.out.println("Generating data");
        long time = System.currentTimeMillis();
        generateData(limit, bits);
        System.out.println("Time: " + (System.currentTimeMillis()-time) + "\n");
        String[] methods = {"simple", "multithread", "akka", "rxjava"};
        for(String method: methods) runMethod(method, threads, limit);
    }
}
