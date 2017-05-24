package ruraomsk.list.ru.strongsql.server;

import ruraomsk.list.ru.strongsql.params.ParamSQL;
import ruraomsk.list.ru.strongsql.params.SetValue;
import ruraomsk.list.ru.strongsql.sql.StrongSql;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Server {
    private int packagesInBlock = Configuration.packagesInBlock;

    private StrongSql sql;
    private ExecutorService workers = Executors.newFixedThreadPool(5);
    private Map<Socket, Date> closeSocketMap = new ConcurrentHashMap<>();
    private Set<Socket> query = Collections.newSetFromMap(new ConcurrentHashMap<Socket, Boolean>());
    private List<SetValue> setValues;

    private ByteBuffer byteBuffer = ByteBuffer.allocate(packagesInBlock * Configuration.bytesInPackage);

    public void getConnection() {
        ParamSQL param = new ParamSQL();
        param.myDB = "temp";
        param.JDBCDriver = "org.postgresql.Driver";
        param.url = "jdbc:postgresql://localhost:5432/testbase";
        param.user = "postgres";
        param.password = "4rfv7YGV";
        sql = new StrongSql(param);
        System.out.println("База " + param.toString() + " открыта...");

        try (ServerSocket serverSocket = new ServerSocket(7777)) {
            monitorTasks();
            while (true) {
                final Socket socket = serverSocket.accept();
                query.add(socket);
                closeSocketMap.put(socket, new Date());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void closeByTimeout(final Socket socket) {
        new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    Calendar calendar = Calendar.getInstance();
                    calendar.add(Calendar.SECOND, (-1 * Configuration.timeout));

                    if (closeSocketMap.get(socket).getTime() < calendar.getTimeInMillis()) {
                        System.out.println("Closing connection by timeout " + socket.getLocalAddress());
                        try {
                            if (!socket.isClosed())
                                socket.close();
                            System.out.println("Closed " + socket.isClosed());
                            closeSocketMap.remove(socket);
                            break;
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        }).start();
    }

    private void monitorTasks() {
        new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    if (!query.isEmpty()) {
                        final Socket currentSocket = query.iterator().next();

                        workers.execute(new Runnable() {
                            @Override
                            public void run() {
                                handle(currentSocket);
                            }
                        });
                        closeByTimeout(currentSocket);
                        query.remove(currentSocket);
                    } else
                        try {
                            Thread.sleep(4000);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                }
            }
        }).start();
    }

    private void handle(Socket socket) {
        try (InputStream reader = socket.getInputStream();
             OutputStream writer = socket.getOutputStream()) {
            ByteBuffer buffer = ByteBuffer.allocate(20);
            reader.read(buffer.array());
            long to = buffer.getLong();
            long from = buffer.getLong();
            int id = buffer.getInt();

            setValues = sql.seekData(new Timestamp(from), new Timestamp(to), id);
            sendData(setValues, writer, socket);
            readReply(reader, writer, socket);
            if (!socket.isClosed())
                closeConnection(socket);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void sendData(List<SetValue> setValues, OutputStream writer, Socket socket) {
        ArrayList<SetValue> sendPart = new ArrayList<>();
        int numberOfBlocks = setValues.size() / packagesInBlock;
        int numberOfBlocksToClient = (setValues.size() % packagesInBlock) == 0 ? numberOfBlocks : numberOfBlocks + 1;
        try {
            if (setValues.size() != 0) {

                for (int j = 0; j < numberOfBlocks - 1; j++) {
                    for (int i = j * packagesInBlock; i < j * packagesInBlock + packagesInBlock; i++)
                        sendPart.add(setValues.get(i));
                    writeToClient((byte) 0, numberOfBlocksToClient, sendPart, writer);
                    sendPart.clear();
                    closeSocketMap.put(socket, new Date());
                }

                if (setValues.size() % packagesInBlock == 0) {
                    for (int i = (numberOfBlocks - 1) * packagesInBlock + 1; i < setValues.size(); i++)
                        sendPart.add(setValues.get(i));
                    writeToClient((byte) 1, numberOfBlocksToClient, sendPart, writer);
                    closeSocketMap.put(socket, new Date());
                } else {
                    for (int i = numberOfBlocks * packagesInBlock + 1; i < setValues.size(); i++)
                        sendPart.add(setValues.get(i));
                    writeToClient((byte) 1, numberOfBlocksToClient, sendPart, writer);
                    closeSocketMap.put(socket, new Date());
                }

            } else {
                byteBuffer.clear();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void readReply(InputStream reader, OutputStream writer, Socket socket) throws IOException {
        ByteBuffer replyFromClient = ByteBuffer.allocate(1);
        reader.read(replyFromClient.array());
        byte reply = replyFromClient.get(0);
        System.out.println("Is ok " + reply);
        if (reply == 0)
            sendData(setValues, writer, socket);
        replyFromClient.clear();
    }

    private void closeConnection(Socket socket) throws IOException {
        ByteBuffer closeFromClient = ByteBuffer.allocate(1);
        InputStream inputStream = socket.getInputStream();
        inputStream.read(closeFromClient.array());
        byte isClose = closeFromClient.get(0);
        if (isClose == 1) {
            System.out.println("Closing connection " + socket.getLocalAddress());
            socket.close();
        }
        System.out.println("Closed " + socket.isClosed());
    }

    private void writeToClient(byte isLast, Integer number, ArrayList<SetValue> sendPart, OutputStream writer) throws IOException {
        byteBuffer.put(isLast);
        byteBuffer.putInt(number);
        byteBuffer.put(sql.getByteArray(sendPart));
        sendPart.clear();
        writer.write(byteBuffer.array());
        byteBuffer.clear();
    }
}

