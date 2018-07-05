package scala
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.client.Admin
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase
import org.apache.hadoop.hbase
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.client.Delete
import org.apache.hadoop.hbase.client.Table
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.client.ResultScanner
import org.apache.hadoop.hbase.client.Result

object HBaseLocalTest {
    def isExists(connection: Connection,tableName:String):Boolean ={
        val admin = connection.getAdmin
        val t = TableName.valueOf(tableName)
        admin.tableExists(t)
    }

  def main(args: Array[String]): Unit = {
    val lll =  HBaseLocalTest
    val conf = HBaseConfiguration.create
    val connection= ConnectionFactory.createConnection(conf)
    print (lll.isExists(connection,"user"))
  }
}