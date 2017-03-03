/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ruraomsk.list.ru.strongsql.tests;

import java.sql.Timestamp;
import java.util.ArrayList;
import ruraomsk.list.ru.strongsql.DescrValue;
import ruraomsk.list.ru.strongsql.ParamSQL;
import ruraomsk.list.ru.strongsql.SetValue;
import ruraomsk.list.ru.strongsql.StrongSql;
import ruraomsk.list.ru.strongsql.Util;

/**
 *
 * @author Yury Rusinov <ruraomsk@list.ru Automatics-A Omsk>
 */
public class TestReader {

    public static void main(String[] args) throws InterruptedException {

        ArrayList<SetValue> arrayValues = new ArrayList<>();
        ArrayList<DescrValue> arraydesc = new ArrayList<>();
        ParamSQL param = new ParamSQL();
        param.myDB = "temp";
        param.JDBCDriver = "org.postgresql.Driver";
        param.url = "jdbc:postgresql://192.168.1.70:5432/testbase";
        param.user = "postgres";
        param.password = "162747";
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
//            for(SetValue sv:arrayValues){
//                System.out.println(sv.toString());
//            }
//            break;
            }
            Long times = (System.currentTimeMillis() - start) / count;
            System.out.println("time on one seek=" + times.toString());
            Thread.sleep(10000L);
            stSQL.disconnect();

        }
    }

}
