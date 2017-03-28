/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ruraomsk.list.ru.strongsql.tests;

import java.sql.Timestamp;
import java.util.ArrayList;

import ruraomsk.list.ru.strongsql.model.DescrValue;
import ruraomsk.list.ru.strongsql.params.ParamSQL;
import ruraomsk.list.ru.strongsql.params.SetValue;
import ruraomsk.list.ru.strongsql.sql.StrongSql;
import ruraomsk.list.ru.strongsql.utils.Util;

/**
 * @author Yury Rusinov <ruraomsk@list.ru Automatics-A Omsk>
 */
public class TestWriters {

    public static void main(String[] args) throws InterruptedException {

        ArrayList<SetValue> arrayValues = new ArrayList<>();
        ArrayList<DescrValue> arrayDesc = new ArrayList<>();
//        arrayDesc.add(new DescrValue("testbool", 1, 0));
//        arrayDesc.add(new DescrValue("testint", 2, 1));
//        arrayDesc.add(new DescrValue("testfloat", 3, 2));
//        arrayDesc.add(new DescrValue("testlong", 4, 3));
        for (Integer i = 1; i < 15; i++) {
            arrayDesc.add(new DescrValue("test" + i.toString(), i, 1));
        }

//       db enterprise
//       StrongSql stSQL=new StrongSql("float", "org.postgresql.Driver", "jdbc:postgresql://127.0.0.1:5432/testbase", "postgres", "162747");

        System.out.println("Начинаем создавать БД");
        ParamSQL param = new ParamSQL();
        param.myDB = "temp";
        param.JDBCDriver = "org.postgresql.Driver";
        param.url = "jdbc:postgresql://localhost:5432/testbase";
        param.user = "postgres";
        param.password = "4rfv7YGV";
        new StrongSql(param, arrayDesc, 0, 5000000L, "description");
        System.out.println("База " + param.toString() + " создана...");

        StrongSql stSQL = new StrongSql(param);

        System.out.println("База " + param.toString() + " открыта...");

            for (DescrValue dsv : stSQL.getNames().values()) {
            SetValue setV = new SetValue(dsv.getId(), Util.emptyValue(dsv.getType()));
            arrayValues.add(setV);
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
