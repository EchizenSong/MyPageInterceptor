package com.demo.eurekaclient.utils;

import com.demo.eurekaclient.common.Page;
import org.apache.ibatis.executor.parameter.ParameterHandler;
import org.apache.ibatis.executor.statement.RoutingStatementHandler;
import org.apache.ibatis.executor.statement.StatementHandler;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.mapping.ParameterMapping;
import org.apache.ibatis.plugin.*;
import org.apache.ibatis.scripting.defaults.DefaultParameterHandler;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

/**
 * @Author: Bryant
 * @Date: Created in 2018/11/1 4:19 PM
 */
//拦截StatementHandler接口中参数类型为connection的prepare方法
@Component
@Intercepts(
        @Signature(
                type = StatementHandler.class,
                method = "prepare",
                args = {Connection.class, Integer.class}
        )
        )
public class MyPageInterceptor implements Interceptor {

    //拦截后需要执行的方法
    /**
    * @Description: mybatis分页核心：使用JDBC操作数据库时，需要Statement对象，其中包含了sql语句，
     *              但sql语句在该对象生成前完成。
     *              在mybatis中，Statement中语句是在RoutingStatementHandler对象的
*                   prepare方法生成的，拦截此方法进行sql的改造
    * @author Bryant
    * @date 2018/11/1 4:36 PM
    * @param
    */
    @Override
    public Object intercept(Invocation invocation) throws Throwable {
        RoutingStatementHandler handler = (RoutingStatementHandler) invocation.getTarget();
        StatementHandler delegate = (StatementHandler) ReflectUtil.getFieldValue(handler, "delegate");
        //BoundSql包含了sql语句以及mapper传参
        BoundSql boundSql = delegate.getBoundSql();
        Object obj = boundSql.getParameterObject();
        if (obj instanceof Page) {
            Page page = (Page) obj;
            //通过反射获取delegate父类BaseStatementHandler的mappedStatement属性
            MappedStatement mappedStatement = (MappedStatement)ReflectUtil.getFieldValue(delegate, "mappedStatement");
            //拦截到的prepare方法参数是一个Connection对象
            Connection connection = (Connection)invocation.getArgs()[0];
            //获取当前要执行的Sql语句，也就是我们直接在Mapper映射语句中写的Sql语句
            String sql = boundSql.getSql();
            //给当前的page参数对象设置总记录数
            this.setTotalRecord(page,
                    mappedStatement, connection);
            //获取分页Sql语句
            String pageSql = this.getPageSql(page, sql);
            //利用反射设置当前BoundSql对应的sql属性为我们建立好的分页Sql语句
            ReflectUtil.setFieldValue(boundSql, "sql", pageSql);
        }
        //继续调用当前方法
        return invocation.proceed();
    }

    /**
    * @Description: 封装分页sql
    * @author Bryant
    * @date 2018/11/1 6:11 PM
    * @param
    */
    private String getPageSql(Page page, String sql) {
        StringBuffer stringBuffer = new StringBuffer(sql);
        //计算第一条记录的位置，Mysql中记录的位置是从0开始的。
        int offset = (page.getPageNo() - 1) * page.getPageSize();
        stringBuffer.append(" limit ").append(offset).append(",").append(page.getPageSize());
        return stringBuffer.toString();
    }

    /**
    * @Description: 设置总数值
    * @author Bryant
    * @date 2018/11/1 6:11 PM
    * @param
    */
    private void setTotalRecord(Page page, MappedStatement mappedStatement, Connection connection) {
            //获取对应的BoundSql，这个BoundSql其实跟我们利用StatementHandler获取到的BoundSql是同一个对象。
            //delegate里面的boundSql也是通过mappedStatement.getBoundSql(paramObj)方法获取到的。
            BoundSql boundSql = mappedStatement.getBoundSql(page);
            //获取到我们自己写在Mapper映射语句中对应的Sql语句
            String sql = boundSql.getSql();
            //通过查询Sql语句获取到对应的计算总记录数的sql语句
            String countSql = this.getCountSql(sql);
            //通过BoundSql获取对应的参数映射
            List<ParameterMapping> parameterMappings = boundSql.getParameterMappings();
            //利用Configuration、查询记录数的Sql语句countSql、参数映射关系parameterMappings和参数对象page建立查询记录数对应的BoundSql对象。
            BoundSql countBoundSql = new BoundSql(mappedStatement.getConfiguration(), countSql, parameterMappings, page);
            //通过mappedStatement、参数对象page和BoundSql对象countBoundSql建立一个用于设定参数的ParameterHandler对象
            ParameterHandler parameterHandler = new DefaultParameterHandler(mappedStatement, page, countBoundSql);
            //通过connection建立一个countSql对应的PreparedStatement对象。
            PreparedStatement pstmt = null;
            ResultSet rs = null;
            try {
                pstmt = connection.prepareStatement(countSql);
                //通过parameterHandler给PreparedStatement对象设置参数
                parameterHandler.setParameters(pstmt);
                //之后就是执行获取总记录数的Sql语句和获取结果了。
                rs = pstmt.executeQuery();
                if (rs.next()) {
                    int totalRecord = rs.getInt(1);
                    //给当前的参数page对象设置总记录数
                    page.setTotalCount(totalRecord);
                }
            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                try {
                    if (rs != null)
                        rs.close();
                    if (pstmt != null)
                        pstmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
    }

    /**
    * @Description: 封装获取总数的sql语句
    * @author Bryant
    * @date 2018/11/1 6:09 PM
    * @param
    */
    private String getCountSql(String sql) {
        return "select count(1) from (" + sql + ")";
    }

    //封装原始对象的方法
    @Override
    public Object plugin(Object o) {
        return Plugin.wrap(o, this);
    }

    @Override
    public void setProperties(Properties properties) {

    }
}
