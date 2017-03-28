package ruraomsk.list.ru.strongsql.tests;/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

import java.sql.Timestamp;
import java.util.ArrayList;

import ruraomsk.list.ru.strongsql.model.DescrValue;
import ruraomsk.list.ru.strongsql.params.ParamSQL;
import ruraomsk.list.ru.strongsql.params.SetValue;
import ruraomsk.list.ru.strongsql.sql.StrongSql;

/**
 * @author Yury Rusinov <ruraomsk@list.ru Automatics-A Omsk>
 */
public class TestReader {

    public static void main(String[] args) throws InterruptedException {

        ArrayList<SetValue> arrayValues;

        ParamSQL param = new ParamSQL();
        param.myDB = "temp";
        param.JDBCDriver = "org.postgresql.Driver";
        param.url = "jdbc:postgresql://localhost:5432/testbase";
        param.user = "postgres";
        param.password = "4rfv7YGV";
        while (true) {
            StrongSql stSQL = new StrongSql(param);
            System.out.println("База " + param.toString() + " открыта...");
            Long start = System.currentTimeMillis();
            Integer count = 0;
            for (DescrValue dsv : stSQL.getNames().values()) {
                count++;

                Timestamp to = new Timestamp(System.currentTimeMillis() - 5000L);

                Timestamp from = new Timestamp(System.currentTimeMillis() - 360000L);

                arrayValues = stSQL.seekData(from, to, dsv.getId());
                for (SetValue sv : arrayValues) {
                    System.out.println(sv.toString());
                }
//            break;
            }
            Long times = (System.currentTimeMillis() - start) / count;
            System.out.println("time on one seek=" + times.toString());
            Thread.sleep(1000L);
            stSQL.disconnect();

        }
    }

}
