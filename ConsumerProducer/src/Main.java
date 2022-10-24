import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.*;


/**
 * Implement Consumer and Producer.
 * @author Kun Liu
 */
public class Main {

    /**
     * Main method.
     * initiate three queue with random length between (0,30]
     * create single producer and three consumers and add to the thread poll
     * The program will keep running until manually interrupted, you can also set a producer limit in MyProducer
     * @param args
     */
    public static void main(String[] args) {
        Random randLen = new Random();
        MyQueue queue1 = new MyQueue(new ArrayBlockingQueue<>(randLen.nextInt(30)), 1);
        MyQueue queue2 = new MyQueue(new ArrayBlockingQueue<>(randLen.nextInt(30)), 2);
        MyQueue queue3 = new MyQueue(new ArrayBlockingQueue<>(randLen.nextInt(30)), 3);

        ArrayList<MyQueue> queueList = new ArrayList<>();

        queueList.add(queue1);
        queueList.add(queue2);
        queueList.add(queue3);
        ExecutorService executorService = Executors.newFixedThreadPool(4);

        MyProducer producer = new MyProducer(queueList, ThreadColor.DEFAULT);
        MyConsumer consumer1 = new MyConsumer(queue1,ThreadColor.GREEN);
        MyConsumer consumer2 = new MyConsumer(queue2, ThreadColor.BLUE);
        MyConsumer consumer3 = new MyConsumer(queue3, ThreadColor.PURPLE);

        executorService.submit(producer);
        executorService.submit(consumer1);
        executorService.submit(consumer2);
        executorService.submit(consumer3);


        Future<String> future = executorService.submit(new Callable<String>() {
            @Override
            public String call() throws Exception {
                System.out.println(ThreadColor.RED +" I am being printed from the callable class");
                return "This is the callable result";
            }
        });

        try{
            System.out.println(future.get());  //.get is a blocking method
        }catch (ExecutionException e){
            System.out.println("Something went wrong");
        }catch (InterruptedException e){
            System.out.println("Thread running the task was interrupted");
        }

        // orderly shutdown the threads
        executorService.shutdown();
    }
}

class MyQueue {
    private ArrayBlockingQueue<String> queue;
    private int id;
    private ThreadColor color;
    public MyQueue(ArrayBlockingQueue<String> queue, int id){
        this.queue = queue;
        this.id = id;
    }

    public ArrayBlockingQueue<String> getQueue(){
        return queue;
    }

    public int getId() {
        return id;
    }
}

/**
 * MyProducer class implements abstract Runnable Class
 * add a random number into according queue
 */
class MyProducer implements Runnable {
    /*a list of MyQueue, a list of buffer queues*/
    private ArrayList<MyQueue> queueList;
    /*thread color to distinguish different threads in the console*/
    private String color;

    /**
     * constructor.
     * @param queueList a list of MyQueue, a list of buffer queues
     * @param color thread color to distinguish different threads in the console
     */
    public MyProducer(ArrayList<MyQueue> queueList, String color) {
        this.queueList = queueList;
        this.color = color;
    }

    /**
     * Every round producer will produce item and add it to the shortest queue
     */
    public void run(){
        Random random = new Random();
        while(true){
            try{
                int shortest_size = Integer.MAX_VALUE;
                ArrayList<MyQueue> same_size_list = new ArrayList<>();
                for(int i = 0; i <queueList.size(); i++) {
                    if(queueList.get(i).getQueue().size() < shortest_size){
                        same_size_list.clear();
                        same_size_list.add(queueList.get(i));
                        shortest_size = queueList.get(i).getQueue().size();
                    }else if(queueList.get(i).getQueue().size() == shortest_size){
                        same_size_list.add(queueList.get(i));
                    }
                }
                // be the shortest queue or random
                int shortest_index = random.nextInt(same_size_list.size());
                MyQueue shortest_q = same_size_list.get(shortest_index);

                ArrayBlockingQueue<String> queue = shortest_q.getQueue();
                int id = shortest_q.getId();
                String num = String.valueOf(random.nextInt(100));
                System.out.println(color + "Adding " + num +" to queue "+id + ". ");
                queue.put(num);
                //put is thread safe method so don't need of synchronization
                Thread.sleep(random.nextInt(1000));

            }catch (InterruptedException e){
                System.out.println("Producer was interrupted");
            }
        }

    }
}


/**
 * MyConsumer class implements abstract Runnable Class
 * add a random number into according queue
 */
class MyConsumer implements Runnable {
    /*queue belongs to the consumer*/
    private ArrayBlockingQueue<String> queue;
    /*thread color to distinguish different threads in the console*/
    private String color;
    /*consumer id, same as queue id*/
    private int id;

    /**
     * constructor.
     * @param queue certain queue belongs to the consumer
     * @param color thread color to distinguish different threads in the console
     */
    public MyConsumer(MyQueue queue, String color) {
        this.queue = queue.getQueue();
        this.color = color;
        this.id = queue.getId();
    }

    /**
     * consumers simultaneously consume from queue
     */
    public void run() {
        Random random = new Random();
        while (true) {
            synchronized (queue) {
                try {
                    Thread.sleep(random.nextInt(1000));
                    if (queue.isEmpty()) {
                        continue;
                    } else {
                        System.out.println(color + "Removed " + queue.take() +" from queue "+id + ". ");;
                    }
                } catch (InterruptedException e) {
                    System.out.println("Consumer was interrupted");
                }
            }
        }
    }
}