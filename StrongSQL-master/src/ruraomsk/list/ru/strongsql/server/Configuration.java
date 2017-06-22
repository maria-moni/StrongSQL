package ruraomsk.list.ru.strongsql.server;

public class Configuration {
    public static int dataInBlock = 7;
    public static int timeout = 60;
    public static int bytesInPackage = 32;// 4 + 8 + 8 + 1 + (isLast 1 + size 4 + crc 8)
}
