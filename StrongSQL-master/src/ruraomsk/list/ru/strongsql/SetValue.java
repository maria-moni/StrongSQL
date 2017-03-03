/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ruraomsk.list.ru.strongsql;

import java.sql.Date;

/**
 * Структура для хранения значений переменных
 *
 * @author Yury Rusinov <ruraomsk@list.ru Automatics-A Omsk>
 */
public class SetValue {

    private Integer id;
    private Long time;
    private Object value;
    private byte good;

    public SetValue(Integer id, Long time, Object value) {
        this.id = id;
        this.time = time;
        this.value = value;
        good = 0;
    }

    public SetValue(Integer id, Long time, Object value, byte good) {
        this.id = id;
        this.time = time;
        this.value = value;
        this.good = good;
    }

    public SetValue(Integer id, Object value) {
        this.id = id;
        this.time = 0L;
        this.value = value;
        good = 0;
    }

    public SetValue(Integer id, Object value, byte good) {
        this.id = id;
        this.time = 0L;
        this.value = value;
        this.good = good;
    }

    @Override
    public String toString() {
        return "=" + id.toString() + " [" + value.toString() + "] " + (time != 0L ? new Date(time).toString() : "") + (good != 0 ? "!" : ""); //To change body of generated methods, choose Tools | Templates.
    }

    /**
     * @return the id
     */
    public Integer getId() {
        return id;
    }

    /**
     * @return the time
     */
    public Long getTime() {
        return time;
    }

    /**
     * @return the value
     */
    public Object getValue() {
        return value;
    }

    public Float getFloatValue() {

        switch (value.getClass().getName()) {
            case "java.lang.Boolean":
                return (boolean) value ? 1.0f : 0.0f;
            case "java.lang.Integer":
                return (float) ((int) value);
            case "java.lang.Float":
                return (float) value;
            case "java.lang.Long":
                return (float) ((long) value & 0xffffffff);
            case "java.lang.Byte":
                return (float) ((int) ((byte) value) & 0xff);
        }
        return 0f;
    }

    /**
     * @param value the value to set
     */
    public void setValue(Object value) {
        this.value = value;
    }

    public void setGood(byte good) {
        this.good = good;
    }

    public byte getGood() {
        return good;
    }

}
