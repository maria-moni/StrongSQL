package ruraomsk.list.ru.strongsql.server;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.sql.Timestamp;

public class Client {

    public static void main(String[] args) {

        Socket socket;
        try {
            socket = new Socket("localhost", 7777);
            OutputStream writer = socket.getOutputStream();

            Timestamp to = new Timestamp(System.currentTimeMillis() - 5000L);
            Timestamp from = new Timestamp(System.currentTimeMillis() - 360000L);

            ByteBuffer byteBuffer = getBuffer(from, to, 1);
            writer.write(byteBuffer.array());

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private static ByteBuffer getBuffer(Timestamp from, Timestamp to, int id){
        ByteBuffer byteBuffer = ByteBuffer.allocate(20);
        byteBuffer.putLong(to.getTime());
        byteBuffer.putLong(from.getTime());
        byteBuffer.putInt(id);
        return byteBuffer;
    }
}
