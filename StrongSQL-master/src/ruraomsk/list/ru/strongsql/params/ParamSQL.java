/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ruraomsk.list.ru.strongsql.params;

/**
 *
 * @author rura
 */
public class ParamSQL {

    public String myDB;
    public String JDBCDriver;
    public String url;
    public String user;
    public String password;

    @Override
    public String toString() {
        return myDB+" "+JDBCDriver+" "+url+" "+user+" "+password;
    }
    
}
