package com.mouse.tool

import java.sql.{Connection, DriverManager}


/**
  * @author 咖啡不加糖
  */
case class InitMySQL(){
      @transient private var connection: Connection = null
      def getMySQLStatement(driver: String,url: String,username: String,password: String):Connection={
          //注册驱动
          Class.forName(driver)
          //得到mysql连接对象
          this.connection = DriverManager.getConnection(url, username, password)
          connection.setAutoCommit(false)// 关闭自动提交事务
          connection
      }


}
