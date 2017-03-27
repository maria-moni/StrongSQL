/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ruraomsk.list.ru.strongsql.utils;

import com.tibbo.aggregate.common.Log;
import ruraomsk.list.ru.strongsql.model.DescrValue;
import ruraomsk.list.ru.strongsql.params.SetValue;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Всевозможные утилиты
 *
 * @author Yury Rusinov <ruraomsk@list.ru Automatics-A Omsk>
 */
public class Util {

    /**
     * Из буфера в короткое целое
     *
     * @param bytes
     * @param idx
     * @return
     */
    public static final int ToShort(byte bytes[], int idx) {
        return (int) (((bytes[idx + 1] << 8) | (bytes[idx] & 0xff)) & 0xffff);
    }

    /**
     * Из буфера в обычное целое
     *
     * @param bytes
     * @param idx
     * @return
     */
    public static final int ToInteger(byte bytes[], int idx) {
        return (int) ((bytes[idx + 3] << 24) | (bytes[idx + 2] << 16) | (bytes[idx + 1] << 8) | (bytes[idx] & 0xff));
    }

    /**
     * Целое 4 байта
     *
     * @param v
     * @return
     */
    public static final byte[] intToRegisters(int v) {
        byte registers[] = new byte[4];
        registers[3] = (byte) (0xff & v >> 24);
        registers[2] = (byte) (0xff & v >> 16);
        registers[1] = (byte) (0xff & v >> 8);
        registers[0] = (byte) (0xff & v);
        return registers;
    }

    /**
     * Целое в буфер записывает два байта
     *
     * @param bytes
     * @param idx
     * @param var
     */
    public static final void ShortToBuff(byte bytes[], int idx, int var) {
        bytes[idx + 1] = (byte) ((var >> 8) & 0x7f);
        bytes[idx] = (byte) (0xff & var);
    }

    /**
     * Целое в буфер записывает четыре байта
     *
     * @param bytes
     * @param idx
     * @param var
     */
    public static final void IntegerToBuff(byte bytes[], int idx, int var) {
        bytes[idx + 3] = (byte) ((var >> 24) & 0xff);
        bytes[idx + 2] = (byte) ((var >> 16) & 0xff);
        bytes[idx + 1] = (byte) ((var >> 8) & 0xff);
        bytes[idx] = (byte) (0xff & var);
    }

    /**
     * Длинное целое в буфкр массива байтов восемь байт
     *
     * @param bytes
     * @param idx
     * @param var
     */
    public static final void LongToBuff(byte bytes[], int idx, long var) {
        bytes[idx + 7] = (byte) (0xff & (var >> 56));
        bytes[idx + 6] = (byte) (0xff & (var >> 48));
        bytes[idx + 5] = (byte) (0xff & (var >> 40));
        bytes[idx + 4] = (byte) (0xff & (var >> 32));
        bytes[idx + 3] = (byte) (0xff & (var >> 24));
        bytes[idx + 2] = (byte) (0xff & (var >> 16));
        bytes[idx + 1] = (byte) (0xff & (var >> 8));
        bytes[idx] = (byte) (0xff & var);
    }

    /**
     * Из буфера извлекает длинное целое
     *
     * @param bytes
     * @param idx
     * @return
     */
    public static final long ToLong(byte bytes[], int idx) {
        return (long) (bytes[idx + 7] & 0xff) << 56 | (long) (bytes[idx + 6] & 0xff) << 48
                | (long) (bytes[idx + 5] & 0xff) << 40 | (long) (bytes[idx + 4] & 0xff) << 32
                | (long) (bytes[idx + 3] & 0xff) << 24 | (long) (bytes[idx + 2] & 0xff) << 16
                | (long) (bytes[idx + 1] & 0xff) << 8 | (long) (bytes[idx] & 0xff);
    }

    /**
     * Из буфера 4 байта в плавающее
     *
     * @param bytes
     * @param idx
     * @return
     */
    public static final float ToFloat(byte bytes[], int idx) {
        return Float.intBitsToFloat((bytes[idx + 3] & 0xff) << 24 | (bytes[idx + 2] & 0xff) << 16 | (bytes[idx + 1] & 0xff) << 8 | bytes[idx] & 0xff);
    }

    /**
     * Плавающее в буфер 4 байта
     *
     * @param bytes
     * @param idx
     * @param f
     */
    public static final void floatToBuff(byte bytes[], int idx, float f) {
        byte registers[] = intToRegisters(Float.floatToIntBits(f));
        System.arraycopy(registers, 0, bytes, idx, registers.length);
    }

    /**
     * Создает буфер для записи в БД
     *
     * @param DBtype - тип хранения данных 0-без временной метки 1-каждая
     * переменная имеет метку времени
     * @param arvalue - массив значений переменных
     * @param ids - МАР описаний переменных
     * @return
     */
    public static byte[] ValuesToBuffer(int DBtype, ArrayList<SetValue> arvalue, HashMap<Integer, DescrValue> ids) {
        int LenBuffer = 0;
        for (SetValue value : arvalue) {
            DescrValue dv = ids.get(value.getId());
            if (dv == null) {
                Log.CORE.info("StrongSQL нет такого id=" + value.getId());
                continue;
            }
            LenBuffer += 6 + dv.getLength();
            if (DBtype == 1) {
                LenBuffer += 8;
            }
        }
        byte[] buffer = new byte[LenBuffer];
        int pos = 0;
        for (SetValue sv : arvalue) {
            DescrValue ds = ids.get(sv.getId());
            if (ds == null) {
                Log.CORE.info("StrongSQL нет такого id=" + sv.getId());
                continue;
            }
            Util.IntegerToBuff(buffer, pos, sv.getId());
            pos += 4;
            buffer[pos++] = (byte) (ds.getLength() & 0xff);
            if (DBtype == 1) {
                Util.LongToBuff(buffer, pos, sv.getTime());
                pos += 8;
            }
            switch (ds.getType()) {
                case 0:
                    buffer[pos++] = (byte) (((boolean) sv.getValue()) ? 1 : 0);
                    break;
                case 1:
                    ShortToBuff(buffer, pos, (int) sv.getValue());
                    pos += 2;
                    break;
                case 2:
                    floatToBuff(buffer, pos, (float) sv.getValue());
                    pos += 4;
                    break;
                case 3:
                    LongToBuff(buffer, pos, (long) sv.getValue());
                    pos += 8;
                    break;
                case 4:

                    if (sv.getValue().getClass().equals("java.lang.Integer")) {
                        buffer[pos++] = (byte) ((int) sv.getValue() & 0xff);
                    } else {
                        buffer[pos++] = (byte) sv.getValue();
                    }
                    break;
            }
            buffer[pos++] = sv.getGood();

        }
        return buffer;
    }

    /**
     * Создает начальное значение перемененой
     *
     * @param type -тип переменной
     * @return - начальное значение
     */
    static public Object emptyValue(int type) {
        switch (type) {
            case 0:
                return false;
            case 1:
                return 0;
            case 2:
                return 0.0f;
            case 3:
                return 0L;
            case 4:
                return (byte) 0;
        }
        return null;
    }

}
