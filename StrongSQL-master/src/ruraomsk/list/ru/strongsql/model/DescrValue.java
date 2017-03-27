/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ruraomsk.list.ru.strongsql.model;

import com.tibbo.aggregate.common.Log;


/**
 * Структура для хранения описаний переменных
 * @author Yury Rusinov <ruraomsk@list.ru Automatics-A Omsk>
 */
public class DescrValue {

    private String name;
    private Integer id;
    private Integer type;

    public DescrValue(String name, Integer id, Integer type) {
        this.name = name;
        this.id = id;
        this.type = type;
    }
//
//    public DescrValue(String name, Integer id, Integer type, Integer lenght) {
//        this.name = name;
//        this.id = id;
//        this.type = type;
//        this.lenght = lenght;
//    }

    @Override
    public String toString() {
        return "id=" + id.toString() + " type=" + type.toString() + " [" + name + "]";
    }

    /**
     * @return the name
     */
    public String getName() {
        return name;
    }

    /**
     * @return the id
     */
    public Integer getId() {
        return id;
    }

    /**
     * @return the type
     */
    public Integer getType() {
        return type;
    }

    /**
     * @return the lenght
     */
    public Integer getLength() {
        switch (type) {
            case 0:
                return 1;
            case 1:
                return 2;
            case 2:
                return 4;
            case 3:
                return 8;
            case 4:
                return 1;
        }
        Log.CORE.info("Ошибка типа в DescrValue");
        return 1;
    }

}
