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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Server {
    private static StrongSql sql;

    public static void main(String[] args) {
        ParamSQL param = new ParamSQL();
        param.myDB = "temp";
        param.JDBCDriver = "org.postgresql.Driver";
        param.url = "jdbc:postgresql://localhost:5432/testbase";
        param.user = "postgres";
        param.password = "4rfv7YGV";
        sql = new StrongSql(param);
        System.out.println("База " + param.toString() + " открыта...");

        final ExecutorService workers = Executors.newFixedThreadPool(5);
        ExecutorService base = Executors.newFixedThreadPool(5);

        try (ServerSocket serverSocket = new ServerSocket(7777)) {
            while (true) {
                final Socket socket = serverSocket.accept();

                base.execute(new Runnable() {
                    @Override
                    public void run() {
                        workers.execute(new Runnable() {
                            @Override
                            public void run() {
                                handle(socket);
                            }
                        });
                    }
                });

            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void handle(Socket socket) {
        try (InputStream reader = socket.getInputStream();
             OutputStream writer = socket.getOutputStream()) {
            ByteBuffer buffer = ByteBuffer.allocate(20);
            reader.read(buffer.array());
            long to = buffer.getLong();
            long from = buffer.getLong();
            int id = buffer.getInt();

            ArrayList<SetValue> sendPart = new ArrayList<>();

            List<SetValue> setValues = sql.seekData(new Timestamp(from), new Timestamp(to), id);
            ByteBuffer byteBuffer = ByteBuffer.allocate(8096);

            if (setValues.size() != 0) {
                sendPart.add(setValues.get(0));
                byte[] sizeOfItem = sql.getByteArray(sendPart);
                sendPart.clear();

                for (SetValue setValue : setValues) {
                    sendPart.add(setValue);

                    if (buffer.position() + sizeOfItem.length < byteBuffer.limit()) {
                        byteBuffer.put(sql.getByteArray(sendPart));
                        sendPart.clear();
                    } else {
                        writer.write(byteBuffer.array());
                        byteBuffer.flip();
                        byteBuffer.put(sql.getByteArray(sendPart));
                    }
                }

                writer.write(byteBuffer.array());
            } else {
                byteBuffer.clear();
                byteBuffer.putInt(-1);
                writer.write(byteBuffer.array());
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
