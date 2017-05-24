package ruraomsk.list.ru.strongsql.server;

public class Configuration {
    public static int packagesInBlock = 7;
    public static int timeout = 60;
    public static int bytesInPackage = 24;// 4 + 8 + 8 + 1 + (isLast 1 + size 4)
}
