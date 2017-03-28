/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ruraomsk.list.ru.strongsql.sql;

import com.tibbo.aggregate.common.Log;
import ruraomsk.list.ru.strongsql.model.DescrValue;
import ruraomsk.list.ru.strongsql.params.ParamSQL;
import ruraomsk.list.ru.strongsql.params.SetData;
import ruraomsk.list.ru.strongsql.params.SetValue;
import ruraomsk.list.ru.strongsql.utils.Util;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public class StrongSql {

    private Long SleepTime = 1000L;
    private Integer DBType;
    private String myDBHead;
    private String myDBHeader;
    private String myDBData;
    private Connection con;
    private TreeMap<String, DescrValue> names;
    private HashMap<Integer, DescrValue> ids;
    private ConcurrentLinkedQueue<SetData> outData;
    private Statement stmt;
    private Long MaxLength;
    private Long TekPos;
    private boolean errorSQL;
    private Long LastPos;
    private ParamSQL paramSQL;
    private WriterSql wrSQL = null;
    private CtrlSql ctrlSQL = null;
    private HashMap<String, Integer> bases;

    /**
     * Начинаем работать с существующей базой данных
     *
     */
    public StrongSql(ParamSQL param) {
        startWork(param);
    }

    public StrongSql(ParamSQL param, Long SleepTime) {
        this.SleepTime = SleepTime;
        startWork(param);
    }

    private void startWork(ParamSQL param) {
        paramSQL = param;
        myDBHead = paramSQL.myDB + "_head";
        myDBHeader = paramSQL.myDB + "_header";
        myDBData = paramSQL.myDB + "_data";
        errorSQL = true;
        outData = new ConcurrentLinkedQueue<>();
        if (connectDB()) {
            errorSQL = false;
            wrSQL = new WriterSql();
            ctrlSQL = new CtrlSql();
        }
    }

    /**
     * Конструктор с созданием списка таблиц
     *
     * @param param
     * @param needbase
     */
    public StrongSql(ParamSQL param, boolean needbase) {
        try {
            paramSQL = param;
            errorSQL = true;
            if (!needbase) {
                return;
            }
            myDBHead = paramSQL.myDB + "_head";
            myDBHeader = paramSQL.myDB + "_header";
            myDBData = paramSQL.myDB + "_data";
            outData = new ConcurrentLinkedQueue<>();
            if (connectUrl()) {
                errorSQL = false;
//                String req = "SELECT tablename FROM pg_tables;";
                bases = new HashMap<>();
                String req = "SELECT table_name FROM information_schema.tables  WHERE table_schema='public' AND table_type='BASE TABLE';";
                ResultSet rs = stmt.executeQuery(req);
                while (rs.next()) {
                    String str = rs.getString(1);
                    if (str.indexOf("_") > 0) {
                        String name = str.substring(0, str.indexOf("_"));
                        bases.put(name, 0);
                    }
                }
                rs.close();
            }
        } catch (ClassNotFoundException | SQLException ex) {
        }
    }

    /**
     * Создаем базу данных на основе массива описаний полей
     *
     * @param arrayDesc - массив описаний переменных
     * @param DBType - тип хранения данных 0-одна временная метка для всех.
     * 1-каждое значение имеет временную метку
     * @param maxSize - максимальное кол-во записей в циклическом буфере
     * @param description - название таблицы в читаемом виде
     */
    public StrongSql(ParamSQL param, ArrayList<DescrValue> arrayDesc, Integer DBType, Long maxSize, String description) {
        try {
            paramSQL = param;
            myDBHead = paramSQL.myDB + "_head";
            myDBHeader = paramSQL.myDB + "_header";
            myDBData = paramSQL.myDB + "_data";
            Class.forName(paramSQL.JDBCDriver);
            con = DriverManager.getConnection(paramSQL.url, paramSQL.user, paramSQL.password);
            stmt = con.createStatement();
            stmt.executeUpdate("drop table if exists " + myDBData + ";");
            stmt.executeUpdate("drop table if exists " + myDBHead + ";");
            stmt.executeUpdate("drop table if exists " + myDBHeader + ";");
            stmt.executeUpdate("create table " + myDBData + " (id bigint,tm timestamp,var bytea, primary key(id));");
            stmt.executeUpdate("create index on " + myDBData + " (tm);");
            stmt.executeUpdate("create table " + myDBHead + " (id int,max bigint,pos bigint,last bigint,type int,description text);");
            stmt.executeUpdate("create table " + myDBHeader + " (id int,name text,type int, primary key(id))");
            stmt.executeUpdate("insert into " + myDBHead + " (id,max,pos,last,type,description) values(1," + maxSize.toString() + ",0,0,"
                    + DBType.toString() + ",'" + description + "');");
            for (DescrValue dsv : arrayDesc) {
                String str = "insert into " + myDBHeader + "(id,name,type) values (" + dsv.getId().toString()
                        + ",'" + dsv.getName() + "'," + dsv.getType().toString() + ");";
                stmt.executeUpdate(str);
            }
            con.commit();
            con.close();
        } catch (ClassNotFoundException | SQLException ex) {
            Log.CORE.info("Error for create DataBase " + ex.getMessage());
        }
    }

    public HashMap<String, Integer> getBases() {
        return bases;
    }

    /**
     * Поиск в базе данных по номерам переменных
     * @param from - время от
     * @param to - время до
     * @param idseek - переменная для поиска
     * @return - возвращает массив значений переменных с метками времени
     */
    public synchronized ArrayList<SetValue> seekData(Timestamp from, Timestamp to, int idseek) {
        try {
            byte[] buffer;
            Integer type = ids.get(idseek).getType();
            ArrayList<SetValue> result = new ArrayList<>();
            HashMap<Long,SetValue> map= new HashMap<>(32000);
            String rez = "SELECT tm,var FROM " + myDBData + " WHERE  tm<='" + to.toString() + "' and tm>='" + from.toString() + "' ORDER BY tm ";
            ResultSet rs = stmt.executeQuery(rez);
            while (rs.next()) {
                Long tm = rs.getTimestamp("tm").getTime();
                buffer = rs.getBytes("var");
                int pos = 0;
                while (pos < buffer.length) {
                    int id = Util.ToInteger(buffer, pos);
                    pos += 4;
                    int l = buffer[pos++];
                    if (l <= 0) {
                        Log.CORE.info("Длина " + l + " у " + id);
                        break;
                    }
                    if (id == idseek) {
                        if (DBType == 1) {
                            tm = Util.ToLong(buffer, pos);
                            pos += 8;
                        }
                        SetValue value = new SetValue(id, tm, 0);
                        switch (type) {
                            case 0:
                                value.setValue((buffer[pos] != 0));
                                break;
                            case 1:
                                value.setValue(Util.ToShort(buffer, pos));
                                break;
                            case 2:
                                value.setValue(Util.ToFloat(buffer, pos));
                                break;
                            case 3:
                                value.setValue(Util.ToLong(buffer, pos));
                                break;
                            case 4:
                                value.setValue(buffer[pos]);
                                break;
                            default:
                                Log.CORE.info("\n Неизвестный тип" + type);

                        }
                        pos += l;
                        value.setGood(buffer[pos++]);
                        map.put(tm,value);
                    } else {
                        if (DBType != 0) {
                            pos += 8;
                        }
                        pos += l;
                        pos++;
                    }

                }
            }
            for(SetValue sv:map.values()){
                result.add(sv);
            }
            rs.close();
            return result;
        } catch (SQLException ex) {
            Log.CORE.info("Ошибка SQL " + ex.getMessage());
            return null;
        }

    }

    /**
     * Установить период записи значений в БД
     *
     * @param SleepTime
     */
    public void setSleepTime(Long SleepTime) {
        this.SleepTime = SleepTime;
    }

    /**
     * Закрывает соединение с базой данных
     */
    public void disconnect() {
        try {
            if (wrSQL == null) {
                return;
            }

            wrSQL.interrupt();
            wrSQL.join(SleepTime * 2);
            ctrlSQL.interrupt();
            ctrlSQL.join(SleepTime * 2);

            con.commit();
            stmt.close();
            con.close();
        } catch (SQLException | InterruptedException ex) {
        }
    }

    private boolean connectUrl() throws ClassNotFoundException, SQLException {
        Class.forName(paramSQL.JDBCDriver);
        con = DriverManager.getConnection(paramSQL.url, paramSQL.user, paramSQL.password);
        stmt = con.createStatement();
        return true;
    }

    private boolean connectDB() {
        try {
            connectUrl();
            String rez = "SELECT * FROM " + myDBHead + " WHERE id=1";
            ResultSet rr = stmt.executeQuery(rez);
            rr.next();
            MaxLength = rr.getLong("max");
            TekPos = rr.getLong("pos");
            LastPos = rr.getLong("last");
            DBType = rr.getInt("type");
            names = new TreeMap();
            ids = new HashMap();
            rez = "SELECT * FROM " + myDBHeader;
            rr = stmt.executeQuery(rez);
            while (rr.next()) {
                DescrValue val = new DescrValue(rr.getString("name"), rr.getInt("id"), rr.getInt("type"));
                names.put(val.getName(), val);
                ids.put(val.getId(), val);
            }
            rr.close();
        } catch (ClassNotFoundException | SQLException ex) {
            Log.CORE.info("Connected " + ex.getMessage());
            return false;
        }
        return true;
    }

    /**
     * Есть ли соединение
     *
     * @return true - база данных дотупна
     */
    public boolean isConnected() {
        return !errorSQL;
    }

    /**
     * Добавить значения переменных в базу данных
     *
     * @param timestamp - временная метка для всей записи
     * @param values - массив значений переменных
     */
    public void addValues(Timestamp timestamp, ArrayList<SetValue> values) {
        byte[] value = Util.ValuesToBuffer(DBType, values, ids);
        SetData setdata = new SetData(timestamp, value);
//        System.out.println(setdata.toString());
        outData.add(setdata);

    }

    /**
     * Получить описания сохраняемых переменных
     *
     * @return - массив описаний переменных
     */
    public TreeMap<String, DescrValue> getNames() {
        return isConnected() ? names : null;
    }

    class WriterSql extends Thread {

        public WriterSql() {
            start();
        }

        @Override
        public void run() {
            while (!isInterrupted()) {
                try {
                    Thread.sleep(SleepTime);
                } catch (InterruptedException ex) {
                    break;
                }
                if (errorSQL) {
                    continue;
                }
                try {
                    con.setAutoCommit(false);
                } catch (SQLException ex) {
                        System.out.println("Error up no commit " + ex.getMessage());
                        errorSQL=true;
                        continue;
                }
                SetData value = null;
                PreparedStatement preparedStatement = null;
                while ((value = outData.poll()) != null) {
//                    System.out.println(value.toString());
                    try {
                        String rez;
                        preparedStatement = con.prepareStatement("begin;");
                        if (LastPos > MaxLength) {
                            // ? - место вставки нашего значеня
                            preparedStatement = con.prepareStatement("UPDATE " + myDBData + " SET tm=? ,var=? WHERE id='" + TekPos.toString() + "';");
                            preparedStatement.setTimestamp(1, value.getTs());
                            preparedStatement.setBytes(2, value.getVar());
                            TekPos++;
                        } else {
                            preparedStatement = con.prepareStatement("INSERT INTO " + myDBData + "(id,tm,var) VALUES( " + TekPos.toString() + ",?,?);");
                            preparedStatement.setTimestamp(1, value.getTs());
                            preparedStatement.setBytes(2, value.getVar());
                            LastPos++;
                            TekPos++;
                        }
                        preparedStatement.executeUpdate();
                        if (TekPos > MaxLength) {
                            TekPos = 0L;
                        }
                        rez = "UPDATE " + myDBHead + " SET pos=" + TekPos.toString() + ", last=" + LastPos.toString() + " WHERE id=1";
                        stmt.executeUpdate(rez);
                        preparedStatement = con.prepareStatement("commit;");
                        preparedStatement.close();
                        con.setAutoCommit(true);
                    } catch (SQLException ex) {
                        try {
                            // Возвращаем обратно данные
                            preparedStatement = con.prepareStatement("rollback;");
                            preparedStatement.close();
                            con.setAutoCommit(true);
                        } catch (SQLException ex1) {
                        }

                        System.out.println("Error writer " + ex.getMessage());
                        outData.add(value);
                        errorSQL = true;
                        continue;
                    }

                }
            }
        }

    }

    class CtrlSql extends Thread {

        public CtrlSql() {
            start();
        }

        @Override
        public void run() {
            while (!isInterrupted()) {
                try {
                    Thread.sleep(SleepTime);
                } catch (InterruptedException ex) {
                    break;
                }
                if (!errorSQL) {
                    continue;
                }
                if (connectDB()) {
                    errorSQL = false;
                }
            }
        }

    }
}
