package tech.mlsql.plugins.ds

import com.alibaba.fastjson.JSON
import com.linkflow.api.LinkflowManager
import org.apache.spark.ml.param.Param
import org.apache.spark.sql.{DataFrame, DataFrameReader, DataFrameWriter, Row, SparkSession}
import org.apache.spark.util.LongAccumulator
import streaming.core.datasource.{DataAuthConfig, DataSinkConfig, DataSourceConfig, DataSourceRegistry, MLSQLDataSourceKey, MLSQLDirectSink, MLSQLRegistry, MLSQLSink, MLSQLSource, MLSQLSourceInfo, MLSQLSparkDataSourceType, SourceInfo}
import streaming.dsl.{ConnectMeta, DBMappingKey, ScriptSQLExec}
import streaming.dsl.mmlib.algs.param.{BaseParams, WowParams}
import tech.mlsql.common.utils.classloader.ClassLoaderTool
import tech.mlsql.common.utils.log.Logging
import tech.mlsql.version.VersionCompatibility

/**
 * @author jayce
 * @date 2022/3/6 9:54 上午
 * @version 1.0
 */
class MLSQLLinkflowApp extends tech.mlsql.app.App with VersionCompatibility with Logging {
  override def supportedVersions: Seq[String] = {
    Seq("1.5.0-SNAPSHOT",
      "1.5.0", "1.6.0-SNAPSHOT",
      "1.6.0", "2.0.0",
      "2.0.1",
      "2.0.1-SNAPSHOT",
      "2.0.1",
      "2.1.0-SNAPSHOT",
      "2.1.0"
    )
  }

  override def run(args: Seq[String]): Unit = {
    val clzz = "tech.mlsql.plugins.ds.MLSQLLinkflow"
    logInfo(s"Load ds: ${clzz}")
    val dataSource = ClassLoaderTool.classForName(clzz).newInstance()
    if (dataSource.isInstanceOf[MLSQLRegistry]) {
      dataSource.asInstanceOf[MLSQLRegistry].register()
    }
  }
}


class MLSQLLinkflow(override val uid: String)
  extends MLSQLSink
    with MLSQLSource
    with MLSQLRegistry
    with WowParams
    with VersionCompatibility
    with Logging {
  def this() = this(BaseParams.randomUID())

  override def fullFormat: String = "linkflow"

  override def shortFormat: String = "tech.mlsql.plugins.ds"

  override def save(writer: DataFrameWriter[Row], config: DataSinkConfig): Unit = {
    val envType = config.path
    var linkflowOptions: Map[String, String] = Map()

    ConnectMeta.presentThenCall(DBMappingKey("linkflow", envType), (options: Map[String, String]) => {
      writer.options(options)
      linkflowOptions = linkflowOptions ++ options
    })
    linkflowOptions = linkflowOptions ++ config.config
    val endpoint: String = linkflowOptions("endpoint").toLowerCase
    val appKey: String = linkflowOptions("appKey")
    val appSecret: String = linkflowOptions("appSecret")
    val timeout: Int = linkflowOptions.getOrElse("timeout", "2000").toInt
    val session: SparkSession = ScriptSQLExec.context().execListener._sparkSession
    // 声明累加器
    val totalAcc: LongAccumulator = session.sparkContext.longAccumulator("totalAcc")
    val failAcc: LongAccumulator = session.sparkContext.longAccumulator("failAcc")
    val successAcc: LongAccumulator = session.sparkContext.longAccumulator("successAcc")
    config.df match {
      case Some(df) =>
        df.foreachPartition((partition: Iterator[Row]) => {
          val linkflowManager = new LinkflowManager(appKey, appSecret, timeout)
          partition.map(row => row.json)
            .foreach(data => {
              totalAcc.add(1)
              endpoint match {
                case "ude" =>
                  if (linkflowManager.insertOrUpdateUde(data)) successAcc.add(1) else failAcc.add(1)
                case "identifiers" =>
                  if (linkflowManager.insertOrUpdateIdentifiers(data)) successAcc.add(1) else failAcc.add(1)
              }
            })
        })
    }
    val resultInfo = s"save linkflow success. totalNum: ${totalAcc.value},successNum: ${successAcc.value},failNum: ${failAcc.value}"
    logInfo(resultInfo)
  }

  override def supportedVersions: Seq[String] = {
    Seq("1.5.0-SNAPSHOT",
      "1.5.0",
      "1.6.0-SNAPSHOT",
      "1.6.0",
      "2.0.0",
      "2.0.1",
      "2.0.1-SNAPSHOT",
      "2.1.0-SNAPSHOT",
      "2.1.0")
  }

  final val endpoint: Param[String] = new Param[String](this, "endpoint", "linkflow endpoint, must not be null")
  final val appKey: Param[String] = new Param[String](this, "appKey", "linkflow appKey, must not be null")
  final val appSecret: Param[String] = new Param[String](this, "appSecret", "linkflow appSecret, must not be null")
  final val timeout: Param[String] = new Param[String](this, "timeout", "linkflow timeout, default 2000ms")

  override def load(reader: DataFrameReader, config: DataSourceConfig): DataFrame = {
    reader.load()
  }

  override def register(): Unit = {
    DataSourceRegistry.register(MLSQLDataSourceKey(fullFormat, MLSQLSparkDataSourceType), this)
    DataSourceRegistry.register(MLSQLDataSourceKey(shortFormat, MLSQLSparkDataSourceType), this)
  }

  override def sourceInfo(config: DataAuthConfig): SourceInfo = {
    val context = ScriptSQLExec.contextGetOrForTest()
    val owner = config.config.get("owner").getOrElse(context.owner)
    SourceInfo(shortFormat, "", "test")
  }

  override def explainParams(spark: SparkSession) = {
    _explainParams(spark)
  }

}

