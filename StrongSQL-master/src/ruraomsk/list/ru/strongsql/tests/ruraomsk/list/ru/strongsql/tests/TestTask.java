package ruraomsk.list.ru.strongsql.tests;

import ruraomsk.list.ru.strongsql.model.DescrValue;
import ruraomsk.list.ru.strongsql.params.ParamSQL;
import ruraomsk.list.ru.strongsql.params.SetValue;
import ruraomsk.list.ru.strongsql.server.Client;
import ruraomsk.list.ru.strongsql.server.Server;
import ruraomsk.list.ru.strongsql.sql.StrongSql;
import ruraomsk.list.ru.strongsql.utils.Util;

import java.sql.Timestamp;
import java.util.ArrayList;

public class TestTask {
    public static void main(String[] args) {
        addValues();
    }

    private static void baseTest() {
        System.out.println("BaseTest");
        new Thread(new Runnable() {
            @Override
            public void run() {
                Server server = new Server();
                server.getConnection();
            }
        }).start();
        try {
            Thread.sleep(3000);
            Client client = new Client(new Timestamp(System.currentTimeMillis()), new Timestamp(0L), 1, false);
            client.connectToServer();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static void closeRequestTest() {
        new Thread(new Runnable() {
            @Override
            public void run() {
                Server server = new Server();
                server.getConnection();
            }
        }).start();
        try {
            Thread.sleep(3000);
            Client client = new Client(new Timestamp(System.currentTimeMillis()), new Timestamp(0L), 1, true);
            client.connectToServer();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static void floatTest() {
        new Thread(new Runnable() {
            @Override
            public void run() {
                Server server = new Server();
                server.getConnection();
            }
        }).start();
        try {
            Thread.sleep(3000);
            Client client = new Client(new Timestamp(System.currentTimeMillis()), new Timestamp(0L), 2, true);
            client.connectToServer();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static void longTest() {
        new Thread(new Runnable() {
            @Override
            public void run() {
                Server server = new Server();
                server.getConnection();
            }
        }).start();
        try {
            Thread.sleep(3000);
            Client client = new Client(new Timestamp(System.currentTimeMillis()), new Timestamp(0L), 3, true);
            client.connectToServer();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static void addValues() {
        ArrayList<SetValue> arrayValues = new ArrayList<>();
        ArrayList<DescrValue> arrayDesc = new ArrayList<>();

        for (Integer i = 1; i < 20; i++) {
            arrayDesc.add(new DescrValue("test" + i.toString(), i, 1));
        }

        for (Integer i = 1; i < 20; i++) {
            arrayDesc.add(new DescrValue("test" + i.toString(), i, 2));
        }

        for (Integer i = 1; i < 20; i++) {
            arrayDesc.add(new DescrValue("test" + i.toString(), i, 3));
        }

        System.out.println("Начинаем создавать БД");
        ParamSQL param = new ParamSQL();
        param.myDB = "temp";
        param.JDBCDriver = "org.postgresql.Driver";
        param.url = "jdbc:postgresql://localhost:5432/testbase";
        param.user = "postgres";
        param.password = "4rfv7YGV";
        new StrongSql(param, arrayDesc, 0, 5000000L, "description");
        System.out.println("База " + param.toString() + " создана.....");
        System.out.println("База " + param.toString() + " открыта...");

        StrongSql stSQL = new StrongSql(param);
        for (DescrValue dsv : stSQL.getNames().values()) {
            SetValue setV = new SetValue(dsv.getId(), Util.emptyValue(dsv.getType()));
            arrayValues.add(setV);
        }
        stSQL.addValues(new Timestamp(System.currentTimeMillis()), arrayValues);

        for (int i = 0; i < 15000; i++) {
            stSQL.addValues(new Timestamp(System.currentTimeMillis()), arrayValues);
        }
        System.out.println("Data added");
    }
}