package ruraomsk.list.ru.strongsql.server;

import java.io.IOException;
import java.net.Socket;

public class Client {
    public static void main(String[] args) {

        Socket socket = new Socket();
        try {
            socket = new Socket("localhost", 7777);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
