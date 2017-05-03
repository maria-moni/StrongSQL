package ruraomsk.list.ru.strongsql.server;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.util.Arrays;

public class Client {
    private int numberInPackage = Configuration.numberInPackage;

    private Timestamp to = new Timestamp(System.currentTimeMillis());
    private Timestamp from = new Timestamp(System.currentTimeMillis() - 360000000000L);
    private int id = 1;
    private byte isLast = 1;
    private byte numberOfPackages;
    private ByteBuffer result;

    public void connectToServer() {
        Socket socket;
        try {
            socket = new Socket("localhost", 7777);
            OutputStream writer = socket.getOutputStream();
            InputStream reader = socket.getInputStream();

            sendRequest(writer);
            readFirstPackage(reader);
            readPackages(reader, writer);

            sendCloseRequest(writer);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private void sendRequest(OutputStream writer) throws IOException {
        ByteBuffer byteBuffer = getBuffer(from, to, id);
        writer.write(byteBuffer.array());
    }

    private void readFirstPackage(InputStream reader) throws IOException {
        ByteBuffer firstPackage = ByteBuffer.allocate(numberInPackage * 17);
        reader.read(firstPackage.array());
        isLast = firstPackage.get();
        if (isLast != 1)
            numberOfPackages = firstPackage.get();
        else numberOfPackages = 1;

        result = ByteBuffer.allocate(numberOfPackages * numberInPackage * 17);
        result.put(firstPackage);
    }

    private void readPackages(InputStream reader, OutputStream writer) throws IOException {
        int readedPackages = 1;
        ByteBuffer packages = ByteBuffer.allocate(numberInPackage * 17);
        if (isLast != 1) {
            while (true) {
                reader.read(packages.array());
                result.put(packages);
                byte isLast = packages.get(0);
                packages.clear();
                readedPackages++;
                if (isLast == 1)
                    break;
            }
        }

        if (readedPackages == numberOfPackages) {
            sendReply(writer, 1);
        } else sendReply(writer, 0);
        System.out.println(Arrays.toString(result.array()));
    }

    private void sendReply(OutputStream writer, int isOk) {
        try {
            ByteBuffer request = ByteBuffer.allocate(1);
            request.put((byte) isOk);
            writer.write(request.array());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void sendCloseRequest(OutputStream writer) {
        try {
            ByteBuffer request = ByteBuffer.allocate(1);
            request.put((byte) 1);
            writer.write(request.array());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private ByteBuffer getBuffer(Timestamp from, Timestamp to, int id) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(20);
        byteBuffer.putLong(to.getTime());
        byteBuffer.putLong(from.getTime());
        byteBuffer.putInt(id);
        return byteBuffer;
    }
}
