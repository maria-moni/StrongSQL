package ruraomsk.list.ru.strongsql.server;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.util.Arrays;

public class Client {

    public void connectToServer() {
        Socket socket;
        try {
            socket = new Socket("localhost", 7777);
            OutputStream writer = socket.getOutputStream();
            InputStream reader = socket.getInputStream();

            Timestamp to = new Timestamp(System.currentTimeMillis());
            Timestamp from = new Timestamp(System.currentTimeMillis() - 360000000000L);

            ByteBuffer byteBuffer = getBuffer(from, to, 1);
            writer.write(byteBuffer.array());

            ByteBuffer buffer = ByteBuffer.allocate(8192);
            reader.read(buffer.array());
            System.out.println(Arrays.toString(buffer.array()));
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private ByteBuffer getBuffer(Timestamp from, Timestamp to, int id){
        ByteBuffer byteBuffer = ByteBuffer.allocate(20);
        byteBuffer.putLong(to.getTime());
        byteBuffer.putLong(from.getTime());
        byteBuffer.putInt(id);
        return byteBuffer;
    }
}
