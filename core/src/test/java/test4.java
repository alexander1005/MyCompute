import com.boraydata.env.Attribute;
import com.boraydata.utils.ProtoStuffSerializer;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

public class test4 {


    public static void main(String[] args) {

        ZContext ctx = new ZContext();
        ZMQ.Socket subscriber = ctx.createSocket(SocketType.SUB);
        subscriber.bind("tcp://*:5557");
        subscriber.subscribe(ZMQ.SUBSCRIPTION_ALL);

        ProtoStuffSerializer protoStuffSerializer = new ProtoStuffSerializer();
        while (true){

            byte[] recv = subscriber.recv();

            Attribute deserializer = protoStuffSerializer.deserializer(recv, Attribute.class);
            System.out.println(deserializer);

        }

    }
}
