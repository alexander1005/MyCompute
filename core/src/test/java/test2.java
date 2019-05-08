import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

public class test2 {


    public static void main(String[] args)
    {
        // Prepare our context and subscriber
        try (ZContext context = new ZContext()) {
            ZMQ.Socket subscriber = context.createSocket(SocketType.SUB);
            subscriber.bind("tcp://*:5563");
            subscriber.subscribe("B".getBytes(ZMQ.CHARSET));
            subscriber.subscribe("A".getBytes(ZMQ.CHARSET));
            while (!Thread.currentThread().isInterrupted()) {
                // Read envelope with address
                String address = subscriber.recvStr();
                // Read message contents
                String contents = subscriber.recvStr();
                //System.out.println(address + " : " + contents);
            }
        }
    }
}
