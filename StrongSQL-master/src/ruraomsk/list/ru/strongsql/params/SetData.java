/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ruraomsk.list.ru.strongsql.params;

import java.sql.Timestamp;
import java.util.Arrays;

/**
 *  Хранение данных для записи в БД
 * @author Yury Rusinov <ruraomsk@list.ru Automatics-A Omsk>
 */
public class SetData {
    private Timestamp ts;
    private byte[] var;

    public SetData(Timestamp ts, byte[] var) {
        this.ts = ts;
        this.var = var;
    }

    @Override
    public String toString() {
        return ts.toString()+Arrays.toString(var); //To change body of generated methods, choose Tools | Templates.
    }

    /**
     * @return the ts
     */
    public Timestamp getTs() {
        return ts;
    }

    /**
     * @return the var
     */
    public byte[] getVar() {
        return var;
    }
    
}
