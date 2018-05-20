#!/bin/bash

exec /usr/bin/spark-shell <<EOF
import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration}
import org.apache.hadoop.hbase.client.{Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat}
import org.apache.hadoop.hbase.util.Bytes
val hconf = HBaseConfiguration.create()
hconf.set("hbase.zookeeper.quorum","kafka1-c1,kafka2-c1,kafka3-c1")
val sourceTable = "wetag:blankmobile"
hconf.set(TableInputFormat.INPUT_TABLE, sourceTable)
val hbaseRDD = sc.newAPIHadoopRDD(hconf, classOf[TableInputFormat],classOf[ImmutableBytesWritable],classOf[Result])
hbaseRDD.cache()
hbaseRDD.count()
val hbaseColums = hbaseRDD.map { case (_, result) =>
      val columns = result.rawCells.map(cell => {
        val cellFamily = Bytes.toString(CellUtil.cloneFamily(cell))
        val cellColumn = Bytes.toString(CellUtil.cloneQualifier(cell))
        cellFamily + ":" + cellColumn
      })
      columns
    }
val columnsRDD = hbaseColums.flatMap({ columns =>
      columns.map(column => {
        (column, 1)
      })
    }).reduceByKey((x, y) => x + y)
val columns = columnsRDD.keys.collect
columns.sorted.foreach(println)
EOF
