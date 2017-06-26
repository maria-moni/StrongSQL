package ruraomsk.list.ru.strongsql.server;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.util.concurrent.TimeUnit;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

public class Client {
    private int packagesInBlock = Configuration.dataInBlock;
    private int bytesInPackage = Configuration.bytesInPackage;
    private long startTime;
    private long stopTime;

    private Timestamp to = new Timestamp(System.currentTimeMillis());
    private Timestamp from = new Timestamp(0L);
    private int id = 1;
    private boolean isClose;
    private byte isLast = 1;
    private Integer numberOfPackages;
    private ByteBuffer result;
    private int q;

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

            startTime = System.nanoTime();
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
        int protocolDataLen = Configuration.protocolExtraDataLen;
        ByteBuffer firstPackage = ByteBuffer.allocate(packagesInBlock * bytesInPackage + protocolDataLen);
        reader.read(firstPackage.array());
        isLast = firstPackage.get();
        numberOfPackages = firstPackage.getInt();
        firstPackage.getLong();
        q = firstPackage.getInt();
        result = ByteBuffer.allocate((numberOfPackages - 1) * packagesInBlock * bytesInPackage + q * bytesInPackage);
        result.put(firstPackage.array(), firstPackage.position(), firstPackage.array().length - protocolDataLen);
        firstPackage.clear();
    }

    private void readPackages(InputStream reader, OutputStream writer) throws IOException {
        int protocolDataLen = Configuration.protocolExtraDataLen;
        int readedPackages = 1;
        ByteBuffer packages;

        long crc = 0;
        if (isLast != 1) {
            while (true) {
                if (readedPackages + 1 == numberOfPackages)
                    packages = ByteBuffer.allocate(q * bytesInPackage + protocolDataLen);
                else
                    packages = ByteBuffer.allocate(packagesInBlock * bytesInPackage + protocolDataLen);
                reader.read(packages.array());
                byte isLast = packages.get();
                packages.getInt();
                crc = packages.getLong();
                packages.getInt();
                readedPackages++;
                result.put(packages.array(), packages.position(), packages.array().length - protocolDataLen);
                packages.clear();
                if (isLast == 1) break;
            }
        }
        stopTime = System.nanoTime();
        System.out.println("Time of full request including answer " + TimeUnit.NANOSECONDS.toMillis(stopTime - startTime) + " ms");

        Checksum checksum = new CRC32();
        checksum.update(result.array(), 0, result.array().length);

//        System.out.println(Arrays.toString(result.array()));
//        System.out.println(result.array().length);
        if (readedPackages == numberOfPackages && crc == checksum.getValue()) {
            sendReply(reader, writer, 1);
        } else sendReply(reader, writer, 0);
    }

    private void sendReply(InputStream reader, OutputStream writer, int isOk) {
        try {
            ByteBuffer request = ByteBuffer.allocate(1);
            request.put((byte) isOk);
            writer.write(request.array());
            if (isOk == 0) {
                readFirstPackage(reader);
                readPackages(reader, writer);
                if (isClose)
                    sendCloseRequest(writer);
            }
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
