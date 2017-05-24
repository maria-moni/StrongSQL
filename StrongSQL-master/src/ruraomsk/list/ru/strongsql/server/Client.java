package ruraomsk.list.ru.strongsql.server;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.util.Arrays;

public class Client {
    private int packagesInBlock = Configuration.packagesInBlock;
    private int bytesInPackage = Configuration.bytesInPackage;

    private Timestamp to = new Timestamp(System.currentTimeMillis());
    private Timestamp from = new Timestamp(0L);
    private int id = 1;
    private boolean isClose;
    private byte isLast = 1;
    private Integer numberOfPackages;
    private ByteBuffer result;

    public Client(Timestamp to, Timestamp from, int id, boolean isClose) {
        this.to = to;
        this.from = from;
        this.id = id;
        this.isClose = isClose;
    }

    public void connectToServer() {
        Socket socket;
        try {
            socket = new Socket("localhost", 7777);
            OutputStream writer = socket.getOutputStream();
            InputStream reader = socket.getInputStream();

            sendRequest(writer);
            readFirstPackage(reader);
            readPackages(reader, writer);
            if (isClose)
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
        ByteBuffer firstPackage = ByteBuffer.allocate(packagesInBlock * bytesInPackage);
        reader.read(firstPackage.array());
        isLast = firstPackage.get(0);
        numberOfPackages = firstPackage.getInt(1);
        result = ByteBuffer.allocate(numberOfPackages * packagesInBlock * bytesInPackage);
        result.put(firstPackage);
        firstPackage.clear();
    }

    private void readPackages(InputStream reader, OutputStream writer) throws IOException {
        int readedPackages = 2;
        ByteBuffer packages = ByteBuffer.allocate(packagesInBlock * bytesInPackage);
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

    public void sendCloseRequest(OutputStream writer) {
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
