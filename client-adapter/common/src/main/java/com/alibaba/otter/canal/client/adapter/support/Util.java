package com.alibaba.otter.canal.client.adapter.support;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.net.JarURLConnection;
import java.net.URL;
import java.net.URLDecoder;
import java.sql.*;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.Date;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

import javax.sql.DataSource;

import com.sun.corba.se.spi.ior.ObjectKey;
import org.apache.commons.lang.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

public class Util {

    private static final Logger logger = LoggerFactory.getLogger(Util.class);

    /**
     * 通过DS执行sql
     */
    public static Object sqlRS(DataSource ds, String sql, Function<ResultSet, Object> fun) {
        try (Connection conn = ds.getConnection();
             Statement stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
            stmt.setFetchSize(Integer.MIN_VALUE);
            try (ResultSet rs = stmt.executeQuery(sql)) {
                return fun.apply(rs);
            }
        } catch (Exception e) {
            logger.error("sqlRs has error, sql: {} ", sql);
            throw new RuntimeException(e);
        }
    }

    public static Object sqlRS(DataSource ds, String sql, List<Object> values, Function<ResultSet, Object> fun) {
        try (Connection conn = ds.getConnection()) {
            try (PreparedStatement pstmt = conn
                    .prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
                pstmt.setFetchSize(Integer.MIN_VALUE);
                if (values != null) {
                    for (int i = 0; i < values.size(); i++) {
                        pstmt.setObject(i + 1, values.get(i));
                    }
                }
                try (ResultSet rs = pstmt.executeQuery()) {
                    return fun.apply(rs);
                }
            }
        } catch (Exception e) {
            logger.error("sqlRs has error, sql: {} ", sql);
            throw new RuntimeException(e);
        }
    }

    /**
     * sql执行获取resultSet
     *
     * @param conn     sql connection
     * @param sql      sql
     * @param consumer 回调方法
     */
    public static void sqlRS(Connection conn, String sql, Consumer<ResultSet> consumer) {
        try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(sql)) {
            consumer.accept(rs);
        } catch (SQLException e) {
            logger.error(e.getMessage(), e);
        }
    }

    public static File getConfDirPath() {
        return getConfDirPath("");
    }

    public static File getConfDirPath(String subConf) {
        URL url = Util.class.getClassLoader().getResource("");
        String path;
        if (url != null) {
            path = url.getPath();
        } else {
            path = new File("").getAbsolutePath();
        }
        File file = null;
        if (path != null) {
            file = new File(
                    path + ".." + File.separator + Constant.CONF_DIR + File.separator + StringUtils.trimToEmpty(subConf));
            if (!file.exists()) {
                file = new File(path + StringUtils.trimToEmpty(subConf));
            }
        }
        if (file == null || !file.exists()) {
            throw new RuntimeException("Config dir not found.");
        }

        return file;
    }

    public static String cleanColumn(String column) {
        if (column == null) {
            return null;
        }
        if (column.contains("`")) {
            column = column.replaceAll("`", "");
        }

        if (column.contains("'")) {
            column = column.replaceAll("'", "");
        }

        if (column.contains("\"")) {
            column = column.replaceAll("\"", "");
        }

        return column;
    }

    public static ThreadPoolExecutor newFixedThreadPool(int nThreads, long keepAliveTime) {
        return new ThreadPoolExecutor(nThreads,
                nThreads,
                keepAliveTime,
                TimeUnit.MILLISECONDS,
                new SynchronousQueue<>(),
                (r, exe) -> {
                    if (!exe.isShutdown()) {
                        try {
                            exe.getQueue().put(r);
                        } catch (InterruptedException e) {
                            // ignore
                        }
                    }
                });
    }

    public static ThreadPoolExecutor newSingleThreadExecutor(long keepAliveTime) {
        return new ThreadPoolExecutor(1,
                1,
                keepAliveTime,
                TimeUnit.MILLISECONDS,
                new SynchronousQueue<>(),
                (r, exe) -> {
                    if (!exe.isShutdown()) {
                        try {
                            exe.getQueue().put(r);
                        } catch (InterruptedException e) {
                            // ignore
                        }
                    }
                });
    }

    public final static String timeZone;    // 当前时区
    private static DateTimeZone dateTimeZone;

    static {
        TimeZone localTimeZone = TimeZone.getDefault();
        int rawOffset = localTimeZone.getRawOffset();
        String symbol = "+";
        if (rawOffset < 0) {
            symbol = "-";
        }
        rawOffset = Math.abs(rawOffset);
        int offsetHour = rawOffset / 3600000;
        int offsetMinute = rawOffset % 3600000 / 60000;
        String hour = String.format("%1$02d", offsetHour);
        String minute = String.format("%1$02d", offsetMinute);
        timeZone = symbol + hour + ":" + minute;
        dateTimeZone = DateTimeZone.forID(timeZone);
        TimeZone.setDefault(TimeZone.getTimeZone("GMT" + timeZone));
    }

    /**
     * 通用日期时间字符解析
     *
     * @param datetimeStr 日期时间字符串
     * @return Date
     */
    public static Date parseDate(String datetimeStr) {
        if (StringUtils.isEmpty(datetimeStr)) {
            return null;
        }
        datetimeStr = datetimeStr.trim();
        if (datetimeStr.contains("-")) {
            if (datetimeStr.contains(":")) {
                datetimeStr = datetimeStr.replace(" ", "T");
            }
        } else if (datetimeStr.contains(":")) {
            datetimeStr = "T" + datetimeStr;
        }

        DateTime dateTime = new DateTime(datetimeStr, dateTimeZone);

        return dateTime.toDate();
    }

    private static LoadingCache<String, DateTimeFormatter> dateFormatterCache = CacheBuilder.newBuilder()
            .build(new CacheLoader<String, DateTimeFormatter>() {

                @Override
                public DateTimeFormatter load(String key) {
                    return DateTimeFormatter.ofPattern(key);
                }
            });

    public static Date parseDate2(String datetimeStr) {
        if (StringUtils.isEmpty(datetimeStr)) {
            return null;
        }
        try {
            datetimeStr = datetimeStr.trim();
            int len = datetimeStr.length();
            if (datetimeStr.contains("-") && datetimeStr.contains(":") && datetimeStr.contains(".")) {
                // 包含日期+时间+毫秒
                // 取毫秒位数
                int msLen = len - datetimeStr.indexOf(".") - 1;
                StringBuilder ms = new StringBuilder();
                for (int i = 0; i < msLen; i++) {
                    ms.append("S");
                }
                String formatter = "yyyy-MM-dd HH:mm:ss." + ms;

                DateTimeFormatter dateTimeFormatter = dateFormatterCache.get(formatter);
                LocalDateTime dateTime = LocalDateTime.parse(datetimeStr, dateTimeFormatter);
                return Date.from(dateTime.atZone(ZoneId.systemDefault()).toInstant());
            } else if (datetimeStr.contains("-") && datetimeStr.contains(":")) {
                // 包含日期+时间
                // 判断包含时间位数
                int i = datetimeStr.indexOf(":");
                i = datetimeStr.indexOf(":", i + 1);
                String formatter;
                if (i > -1) {
                    formatter = "yyyy-MM-dd HH:mm:ss";
                } else {
                    formatter = "yyyy-MM-dd HH:mm";
                }

                DateTimeFormatter dateTimeFormatter = dateFormatterCache.get(formatter);
                LocalDateTime dateTime = LocalDateTime.parse(datetimeStr, dateTimeFormatter);
                return Date.from(dateTime.atZone(ZoneId.systemDefault()).toInstant());
            } else if (datetimeStr.contains("-")) {
                // 只包含日期
                String formatter = "yyyy-MM-dd";
                DateTimeFormatter dateTimeFormatter = dateFormatterCache.get(formatter);
                LocalDate localDate = LocalDate.parse(datetimeStr, dateTimeFormatter);
                return Date.from(localDate.atStartOfDay().atZone(ZoneId.systemDefault()).toInstant());
            } else if (datetimeStr.contains(":")) {
                // 只包含时间
                String formatter;
                if (datetimeStr.contains(".")) {
                    // 包含毫秒
                    int msLen = len - datetimeStr.indexOf(".") - 1;
                    StringBuilder ms = new StringBuilder();
                    for (int i = 0; i < msLen; i++) {
                        ms.append("S");
                    }
                    formatter = "HH:mm:ss." + ms;
                } else {
                    // 判断包含时间位数
                    int i = datetimeStr.indexOf(":");
                    i = datetimeStr.indexOf(":", i + 1);
                    if (i > -1) {
                        formatter = "HH:mm:ss";
                    } else {
                        formatter = "HH:mm";
                    }
                }
                DateTimeFormatter dateTimeFormatter = dateFormatterCache.get(formatter);
                LocalTime localTime = LocalTime.parse(datetimeStr, dateTimeFormatter);
                LocalDate localDate = LocalDate.of(1970, Month.JANUARY, 1);
                LocalDateTime localDateTime = LocalDateTime.of(localDate, localTime);
                return Date.from(localDateTime.atZone(ZoneId.systemDefault()).toInstant());
            }
        } catch (Throwable e) {
            logger.error(e.getMessage(), e);
        }

        return null;
    }

    public static Set<Class<?>> getClasses(String pack) {

        // 第一个class类的集合
        Set<Class<?>> classes = new LinkedHashSet<Class<?>>();
        // 是否循环迭代
        boolean recursive = true;
        // 获取包的名字 并进行替换
        String packageName = pack;
        String packageDirName = packageName.replace('.', '/');
        // 定义一个枚举的集合 并进行循环来处理这个目录下的things
        Enumeration<URL> dirs;
        try {
            dirs = Thread.currentThread().getContextClassLoader().getResources(
                    packageDirName);
            // 循环迭代下去
            while (dirs.hasMoreElements()) {
                // 获取下一个元素
                URL url = dirs.nextElement();
                // 得到协议的名称
                String protocol = url.getProtocol();
                // 如果是以文件的形式保存在服务器上
                if ("file".equals(protocol)) {
                    //System.err.println("file类型的扫描");
                    // 获取包的物理路径
                    String filePath = URLDecoder.decode(url.getFile(), "UTF-8");
                    // 以文件的方式扫描整个包下的文件 并添加到集合中
                    findAndAddClassesInPackageByFile(packageName, filePath,
                            recursive, classes);
                } else if ("jar".equals(protocol)) {
                    // 如果是jar包文件
                    // 定义一个JarFile
                    //System.err.println("jar类型的扫描");
                    JarFile jar;
                    try {
                        // 获取jar
                        jar = ((JarURLConnection) url.openConnection())
                                .getJarFile();
                        // 从此jar包 得到一个枚举类
                        Enumeration<JarEntry> entries = jar.entries();
                        // 同样的进行循环迭代
                        while (entries.hasMoreElements()) {
                            // 获取jar里的一个实体 可以是目录 和一些jar包里的其他文件 如META-INF等文件
                            JarEntry entry = entries.nextElement();
                            String name = entry.getName();
                            // 如果是以/开头的
                            if (name.charAt(0) == '/') {
                                // 获取后面的字符串
                                name = name.substring(1);
                            }
                            // 如果前半部分和定义的包名相同
                            if (name.startsWith(packageDirName)) {
                                int idx = name.lastIndexOf('/');
                                // 如果以"/"结尾 是一个包
                                if (idx != -1) {
                                    // 获取包名 把"/"替换成"."
                                    packageName = name.substring(0, idx)
                                            .replace('/', '.');
                                }
                                // 如果可以迭代下去 并且是一个包
                                if ((idx != -1) || recursive) {
                                    // 如果是一个.class文件 而且不是目录
                                    if (name.endsWith(".class")
                                            && !entry.isDirectory()) {
                                        // 去掉后面的".class" 获取真正的类名
                                        String className = name.substring(
                                                packageName.length() + 1, name
                                                        .length() - 6);
                                        try {
                                            // 添加到classes
                                            classes.add(Class
                                                    .forName(packageName + '.'
                                                            + className));
                                        } catch (ClassNotFoundException e) {
                                            // log
                                            // .error("添加用户自定义视图类错误 找不到此类的.class文件");
                                            e.printStackTrace();
                                        }
                                    }
                                }
                            }
                        }
                    } catch (IOException e) {
                        // log.error("在扫描用户定义视图时从jar包获取文件出错");
                        e.printStackTrace();
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return classes;
    }

    /**
     * 以文件的形式来获取包下的所有Class
     *
     * @param packageName
     * @param packagePath
     * @param recursive
     * @param classes
     */
    public static void findAndAddClassesInPackageByFile(String packageName,
                                                        String packagePath, final boolean recursive, Set<Class<?>> classes) {
        // 获取此包的目录 建立一个File
        File dir = new File(packagePath);
        // 如果不存在或者 也不是目录就直接返回
        if (!dir.exists() || !dir.isDirectory()) {
            // log.warn("用户定义包名 " + packageName + " 下没有任何文件");
            return;
        }
        // 如果存在 就获取包下的所有文件 包括目录
        File[] dirfiles = dir.listFiles(new FileFilter() {
            // 自定义过滤规则 如果可以循环(包含子目录) 或则是以.class结尾的文件(编译好的java类文件)
            public boolean accept(File file) {
                return (recursive && file.isDirectory())
                        || (file.getName().endsWith(".class"));
            }
        });
        // 循环所有文件
        for (File file : dirfiles) {
            // 如果是目录 则继续扫描
            if (file.isDirectory()) {
                findAndAddClassesInPackageByFile(packageName + "."
                                + file.getName(), file.getAbsolutePath(), recursive,
                        classes);
            } else {
                // 如果是java类文件 去掉后面的.class 只留下类名
                String className = file.getName().substring(0,
                        file.getName().length() - 6);
                try {
                    // 添加到集合中去
                    //classes.add(Class.forName(packageName + '.' + className));
                    //经过回复同学的提醒，这里用forName有一些不好，会触发static方法，没有使用classLoader的load干净
                    classes.add(Thread.currentThread().getContextClassLoader().loadClass(packageName + '.' + className));
                } catch (ClassNotFoundException e) {
                    // log.error("添加用户自定义视图类错误 找不到此类的.class文件");
                    e.printStackTrace();
                }
            }
        }
    }


    public static List<Map<String, Object>> getQueryDate(ResultSet rs) {
        List<Map<String, Object>> list = new ArrayList<>();
        try {
            ResultSetMetaData md = rs.getMetaData(); //获得结果集结构信息,元数据
            int columnCount = md.getColumnCount();   //获得列数
            while (rs.next()) {
                Map<String, Object> rowData = new HashMap<String, Object>();
                for (int i = 1; i <= columnCount; i++) {
                    rowData.put(md.getColumnName(i), rs.getObject(i));
                }
                list.add(rowData);

            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return list;
    }


    public static List<Map<String, Object>> getFieldDate(List<Map<String, Object>> queryDate, String k, List<String> fieldList) {
        List<Map<String, Object>> mapList = new ArrayList<>();
        queryDate.forEach(ele -> {
            Map<String, Object> m = new HashMap<>();
            fieldList.forEach(key -> {
                m.put(key, ele.get(key));
            });
            mapList.add(m);
        });
        return mapList;
    }
}
