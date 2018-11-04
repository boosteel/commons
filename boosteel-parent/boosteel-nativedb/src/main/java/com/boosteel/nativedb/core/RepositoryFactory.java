package com.boosteel.nativedb.core;

import com.boosteel.nativedb.NativeDB;
import com.boosteel.nativedb.core.anno.*;
import com.boosteel.nativedb.core.support.Pager;
import com.boosteel.util.IAccess;
import com.boosteel.util.support.MapAccess;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.boosteel.nativedb.core.DataConverter.data_by_jType;

public class RepositoryFactory {

    // @Value용 객체
    private static class QVal {
        int index;
        String key;

        QVal(int i, String k) {
            index = i;
            key = k;
        }
    }

    private static class MethodData {

        NativeDB db;
        int pagerIndex = -1;
        int statementIndex = -1;
        int connectionIndex = -1;
        int mapIndex = -1;

        List<QVal> values = new ArrayList<>();

        /*
         *  Method의 모든 정보를 뽑아낸다.
         *
         *  리턴타입
         *  ① List<Map<String, Object>>
         *  ② Map<String, Object>
         *  ③ int
         *  ④ long
         *  ⑤ String
         *  ⑥ java.util.Date
         *  ⑦ Pager
         *
         *
         *  파라미터로 허용되는건
         *  ① @Value
         *  ③ Map<String, Object>
         *  ④ Connection
         *  ⑤ Statement
         *  ⑥ Pager
         */
        MethodData(Method method, NativeDB db) throws Exception {


            this.db = db;

            int i = 0;
            Class<?>[] parameters = method.getParameterTypes();
            Annotation[][] annotations = method.getParameterAnnotations();

            /*
             *  메소드 파라미터 순회
             */
            for (Class<?> param : parameters) {

                // @Value
                if (annotations[i].length == 1 && Value.class.isAssignableFrom(annotations[i][0].getClass())) {
                    values.add(new QVal(i, ((Value) annotations[i][0]).value()));
                }
                // Pager
                else if (Pager.class.isAssignableFrom(param)) {
                    pagerIndex = i;
                }
                // Map
                else if (Map.class.isAssignableFrom(param)) {
                    mapIndex = i;
                }
                // Connection
                else if (Connection.class.isAssignableFrom(param)) {
                    connectionIndex = i;
                }
                // Statement
                else if (Statement.class.isAssignableFrom(param)) {
                    statementIndex = i;
                }
                // error
                else {
                    throw new RuntimeException(param + " 은 지원하지 않는 Parameter입니다.");
                }
                i++;
            }
        }


        Object doStmt(Object[] args, DoStmt r) throws Exception {
            // Statement가 인자로 제공될때
            if (statementIndex != -1) {
                return r.run((Statement) args[statementIndex]);
            }
            // Connection이 인자로 제공될때
            else if (connectionIndex != -1) {
                try (Statement stmt = ((Connection) args[connectionIndex]).createStatement()) {
                    return r.run(stmt);
                }
            }
            // 아무것도 없을때
            else {
                return db.doStmtR(stmt -> r.run(stmt));
            }
        }

        IAccess<?> getAccess(Object[] args) {
            Map<String, Object> map = mapIndex != -1 ?
                    new HashMap<>((Map<String, Object>) args[mapIndex]) :
                    new HashMap<>();
            for (QVal v : values) map.put(v.key, args[v.index]);
            return new MapAccess(map);
        }
    }


    public interface DoStmt {
        Object run(Statement stmt) throws Exception;
    }

    public interface MethodRun {
        Object run(Object[] args) throws Exception;

    }

    private static final <T> T _log(T t) {
        System.out.println("\n-----------------------------------------------------------------");
        System.out.println(t);
        System.out.println("-----------------------------------------------------------------\n");
        return t;
    }


    public static final <T> T createRepository(Class<T> cons, NativeDB db) {

        try {

            Map<Method, MethodRun> result = new HashMap<>();

            for (Method method : cons.getMethods()) {

                Class<?> returnType = method.getReturnType();
                MethodData md = new MethodData(method, db);

                Insert INSERT;
                Update UPDATE;
                Selector SELECTOR;
                Save SAVE;
                PageList PAGE_LIST;
                SQLString SQL_STRING;

                // @Save
                if ((SAVE = method.getAnnotation(Save.class)) != null) {
                    boolean wantLastId = SAVE.lastId();
                    SQL.DQuery query = SQL.dynamicSQL(SAVE.value());

                    result.put(method, args ->
                            md.doStmt(args, stmt -> {
                                int i = stmt.executeUpdate(_log(query.apply(md.getAccess(args))));
                                if (wantLastId) {
                                    try (ResultSet rs = stmt.executeQuery("SELECT LAST_INSERT_ID()")) {
                                        rs.next();
                                        i = rs.getInt(1);
                                    }
                                }
                                return i;
                            }));
                }
                // @Update
                else if ((UPDATE = method.getAnnotation(Update.class)) != null) {

                    String tableName = UPDATE.value(),
                            sql = "UPDATE " + tableName + " SET ";

                    SQL.DQuery where = SQL.dynamicSQL(method.getAnnotation(Update.class).where());
                    TableInfo info = db.getTableInfo(tableName);

                    result.put(method, args ->
                            md.doStmt(args, stmt -> {
                                IAccess access = md.getAccess(args);
                                return stmt.executeUpdate(_log(
                                        sql + SQL.update(access, info) + " WHERE " +
                                                where.apply(access)));
                            }));
                }

                // @Insert
                else if ((INSERT = method.getAnnotation(Insert.class)) != null) {
                    boolean wantLastId = INSERT.lastId();
                    String tableName = INSERT.value(),
                            sql = "INSERT INTO " + tableName + " ";
                    TableInfo info = db.getTableInfo(tableName);

                    result.put(method, args -> md.doStmt(args, stmt -> {
                        int i = stmt.executeUpdate(_log(sql + SQL.insert(md.getAccess(args), info)));
                        if (wantLastId) {
                            try (ResultSet rs = stmt.executeQuery("SELECT LAST_INSERT_ID()")) {
                                rs.next();
                                i = rs.getInt(1);
                            }
                        }
                        return i;
                    }));
                }

                // @SQLString
                else if ((SQL_STRING = method.getAnnotation(SQLString.class)) != null) {

                    if (!String.class.isAssignableFrom(returnType))
                        throw new RuntimeException("@SQLString error : \n" +
                                "반환값은 반드시 java.lang.String이어야 합니다.");

                    SQL.DQuery query = SQL.dynamicSQL(SQL_STRING.value());
                    result.put(method, (args) -> query.apply(md.getAccess(args)));
                }

                // @PageList
                else if ((PAGE_LIST = method.getAnnotation(PageList.class)) != null) {


                    if (!Pager.class.isAssignableFrom(returnType))
                        throw new RuntimeException("@PageList error : \n" +
                                "리턴값은 반드시 Pager객체이어야 합니다.");
                    if(md.pagerIndex == -1)
                        throw new RuntimeException("@PageList error : \n" +
                                "Pager객체가 반드시 인자로 제공되어야 합니다.");

                    SQL.DQuery $select = SQL.dynamicSQL(PAGE_LIST.list()),
                            $count = SQL.dynamicSQL(PAGE_LIST.count());

                    result.put(method, (args) -> md.doStmt(args, stmt -> {
                        Pager pager = (Pager) args[md.pagerIndex];
                        IAccess access = md.getAccess(args);
                        String select = _log($select.apply(access) + pager.orderBy() + pager.limit()),
                                count = _log($count.apply(access));

                        pager.setContents(DataAccess.readAll(stmt.executeQuery(select)));
                        ResultSet rs = stmt.executeQuery(count);
                        rs.next();
                        return pager.setTotalElements(rs.getLong(1));
                    }));
                }

                // @Selector
                else if ((SELECTOR = method.getAnnotation(Selector.class)) != null) {

                    String type = returnType.getName();
                    SQL.DQuery query = SQL.dynamicSQL(SELECTOR.value());

                    // return ResultSet
                    if (ResultSet.class.isAssignableFrom(returnType)) {
                        result.put(method, args ->
                                md.doStmt(args, stmt -> stmt.executeQuery(_log(query.apply(md.getAccess(args))))));
                    }
                    // return Map<String, Object>
                    else if (Map.class.isAssignableFrom(returnType)) {
                        result.put(method, (args) ->
                                md.doStmt(args, stmt -> {
                                    try (ResultSet rs = stmt.executeQuery(_log(query.apply(md.getAccess(args))))) {
                                        rs.next();
                                        return DataAccess.read(rs, new MapAccess(new HashMap<>()));
                                    }
                                })
                        );
                    }
                    // return List<Map<String, Object>>
                    else if (List.class.isAssignableFrom(returnType)) {
                        result.put(method, (args) ->
                                md.doStmt(args, stmt -> {
                                    try (ResultSet rs = stmt.executeQuery(_log(query.apply(md.getAccess(args))))) {
                                        return DataAccess.readAll(rs);
                                    }
                                })
                        );
                    }
                    // return JAVA DEFAULT TYPE
                    else if (type.indexOf(".") == -1 || type.startsWith("java.lang")) {
                        result.put(method, (args) -> md.doStmt(args, stmt -> {
                            try (ResultSet rs = stmt.executeQuery(_log(query.apply(md.getAccess(args))))) {
                                rs.next();
                                return data_by_jType(rs, rs.getMetaData(), type, 1);
                            }
                        }));
                    }

                    // error
                    else {
                        throw new RuntimeException("@Selector error : \n" +
                                type + " 은 지원하지 않는 반환값입니다.");
                    }
                }
                // error
                else {
                    throw new RuntimeException(method + " 시그니처를 확인하세요.");
                }
            }


            return (T) Proxy.newProxyInstance(
                    cons.getClassLoader(),
                    new Class[]{cons},
                    (proxy, method, methodArgs) -> result.get(method).run(methodArgs));

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
