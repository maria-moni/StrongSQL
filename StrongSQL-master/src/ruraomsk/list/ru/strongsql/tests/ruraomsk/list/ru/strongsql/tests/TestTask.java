package ruraomsk.list.ru.strongsql.tests;

import ruraomsk.list.ru.strongsql.server.Client;
import ruraomsk.list.ru.strongsql.server.Server;

public class TestTask {
    public static void main(String[] args) {
        new Thread(new Runnable() {
            @Override
            public void run() {
                Server server = new Server();
                server.getConnection();
            }
        }).start();
        try {
            Thread.sleep(3000);
            Client client = new Client();
            client.connectToServer();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }
}
