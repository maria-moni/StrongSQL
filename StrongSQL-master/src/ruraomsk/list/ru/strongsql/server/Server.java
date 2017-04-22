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
    private StrongSql sql;
    private ExecutorService workers = Executors.newFixedThreadPool(5);
    private Set<Socket> query = Collections.newSetFromMap(new ConcurrentHashMap<Socket, Boolean>());

    private ByteBuffer byteBuffer = ByteBuffer.allocate(8 * 17); // 4 + 8 + 4 + 1 + (isLast 1 + size 1)
    private List<SetValue> setValues = new ArrayList<>();

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
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void monitorTasks(){
        new Thread(new Runnable() {
            @Override
            public void run() {
                while (true){
                    if (!query.isEmpty()) {
                        final Socket currentSocket = query.iterator().next();
                        workers.execute(new Runnable() {
                            @Override
                            public void run() {
                                handle(currentSocket);
                            }
                        });
                        query.remove(currentSocket);
                    } else
                        try {
                            Thread.sleep(5000);
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
            sendData(setValues, writer);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void sendData(List<SetValue> setValues, OutputStream writer) {
        ArrayList<SetValue> sendPart = new ArrayList<>();
        int numberOfBlocks = setValues.size() / 8;

        try {
            if (setValues.size() != 0) {

                for (int j = 0; j < numberOfBlocks - 1; j++) {
                    for (int i = j * 8; i < j * 8 + 8; i++)
                        sendPart.add(setValues.get(i));
                    writeToClient((byte) 0, (byte) 0, sendPart, writer);
                }

                if (setValues.size() % 8 == 0) {
                    for (int i = (numberOfBlocks - 1) * 8 + 1; i < setValues.size(); i++)
                        sendPart.add(setValues.get(i));
                    writeToClient((byte) 1, (byte) numberOfBlocks, sendPart, writer);
                }

                if (setValues.size() > numberOfBlocks * 8)
                    for (int i = numberOfBlocks * 8 + 1; i < setValues.size(); i++)
                        sendPart.add(setValues.get(i));

                writeToClient((byte) 1, (byte) (numberOfBlocks + 1), sendPart, writer);
            } else {
                byteBuffer.clear();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void writeToClient(byte isLast, byte number, ArrayList<SetValue> sendPart, OutputStream writer) throws IOException {
        byteBuffer.put(isLast);
        byteBuffer.put(number);
        byteBuffer.put(sql.getByteArray(sendPart));
        sendPart.clear();
        writer.write(byteBuffer.array());
        byteBuffer.flip();
    }
}

