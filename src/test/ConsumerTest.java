package test;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;

public class ConsumerTest implements Runnable {
    private KafkaStream m_stream;
    private int m_threadNumber;
    private HashMap<String, Integer> counter = new HashMap<>();
    
    public ConsumerTest(KafkaStream a_stream, int a_threadNumber) {
        m_threadNumber = a_threadNumber;
        m_stream = a_stream;
    }
 
    public void run() {
        ConsumerIterator<byte[], byte[]> it = m_stream.iterator();
        String message;
        while (it.hasNext()){
        	message = new String(it.next().message());
//        	Contando o número de vezes que uma cidade aparece ao longo de toda a stream
        	if(counter.containsKey(message)){
        	     counter.put(message, counter.get(message)+1);
        	}
        	else{
        	    counter.put(message, 1);
        	}
        }
        
        System.out.println(counter);
        System.out.println("Shutting down Thread: " + m_threadNumber);
    }
}