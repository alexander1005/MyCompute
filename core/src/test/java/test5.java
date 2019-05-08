import com.boraydata.env.Attribute;
import com.boraydata.utils.ProtoStuffSerializer;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

public class test5 {

    public static void main(String[] args) {


        ProtoStuffSerializer protoStuffSerializer = new ProtoStuffSerializer();

        ZContext ctx = new ZContext();
        ZMQ.Socket subscriber = ctx.createSocket(SocketType.PUB);
        subscriber.connect("tcp://localhost:5557");
        //subscriber.subscribe(ZMQ.SUBSCRIPTION_ALL);

        while (true){

            Attribute attribute = new Attribute();
            attribute.setValue(123);
            subscriber.send(protoStuffSerializer.serializer(attribute));
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }


    }
}
