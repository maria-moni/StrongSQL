/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ruraomsk.list.ru.strongsql.tests;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import ruraomsk.list.ru.strongsql.*;

/**
 *
 * @author Yury Rusinov <ruraomsk@list.ru Automatics-A Omsk>
 */
public class TestWriters {

    public static void main(String[] args) throws InterruptedException {

        ArrayList<SetValue> arrayValues = new ArrayList<>();
        ArrayList<DescrValue> arraydesc = new ArrayList<>();
//        arraydesc.add(new DescrValue("testbool", 1, 0));
//        arraydesc.add(new DescrValue("testint", 2, 1));
//        arraydesc.add(new DescrValue("testfloat", 3, 2));
//        arraydesc.add(new DescrValue("testlong", 4, 3));
        for (Integer i = 1; i < 15; i++) {
            arraydesc.add(new DescrValue("test" + i.toString(), i, 1));
        }

//        StrongSql stSQL=new StrongSql("float", "org.postgresql.Driver", "jdbc:postgresql://127.0.0.1:5432/testbase", "postgres", "162747");
        System.out.println("Начинаем создавать БД");
        ParamSQL param=new ParamSQL();
        param.myDB="temp";
        param.JDBCDriver="org.postgresql.Driver";
        param.url="jdbc:postgresql://192.168.1.70:5432/testbase";
        param.user="postgres";
        param.password="162747";
        new StrongSql(param, arraydesc, 0, 5000000L, "description");
        System.out.println("База "+param.toString()+" создана...");

        StrongSql stSQL = new StrongSql(param);

        System.out.println("База "+param.toString()+" открыта...");

        for (DescrValue dsv : stSQL.getNames().values()) {
//            System.out.println(dsv.toString());
            SetValue setV = new SetValue(dsv.getId(), Util.emptyValue(dsv.getType()));
            arrayValues.add(setV);
//            System.out.println(setV.toString());
        }
        Integer count = 0;
        while (true) {
            count++;
            if (count > 32000) {
                count = -20000;
            }
            Thread.sleep(1000L);
            for (SetValue sv : arrayValues) {
                sv.setValue(count);
            }
            stSQL.addValues(new Timestamp(System.currentTimeMillis()), arrayValues);
            if ((count % 100) == 0) {
                System.out.println("Count=" + count.toString());
            }
        }
    }
}
